"""Cumulative, as-of Gold reports. AWS Glue 5.1 / Spark 3.5.

One immutable output generation per run, published via catalog partitions only
after all outputs and reconciliation checks succeed. Readers select one as_of_date.
No IBNR estimate, tax, investment income or allocation of overhead to policies.
"""
import json
import sys
import uuid
from datetime import date, timedelta

DATASETS = ('claims_events', 'daily_exposure', 'claim_payments', 'expense_transactions')
DATABASE = 'auto_insurance_gold'
TABLES = ('claim_snapshot', 'policy_performance', 'portfolio_performance', 'expense_summary')
PAYMENT_FIELDS = 'payment_id claim_id policy_id payment_date payment_type payee_type amount currency source_event_id source_version source_system created_at updated_at'.split()
EXPENSE_FIELDS = 'expense_id expense_date expense_category expense_scope amount currency claim_id policy_id region source_system created_at updated_at'.split()
CLAIM_FIELDS = 'claim_id policy_id source_version source_updated_at incident_date reported_date operation claim_status incurred_amount paid_amount'.split()
EXPOSURE_FIELDS = 'policy_id load_date region customer_segment age_band earned_exposure earned_premium'.split()


def dates_between(start, end):
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first.isoformat() != start or last.isoformat() != end or first > last:
        raise ValueError('Expected YYYY-MM-DD dates with HISTORY_START_DATE <= PROCESS_DATE')
    return [(first + timedelta(days=i)).isoformat() for i in range((last-first).days+1)]


def require_empty(df, message):
    rows = df.limit(5).collect()
    if rows:
        raise ValueError(message + ': ' + str([r.asDict() for r in rows]))


def deduplicate(df, fields, keys, label):
    # Immutable business records repeated in different load partitions count once.
    from pyspark.sql import functions as F
    unique = df.select(*fields).dropDuplicates(fields)
    require_empty(unique.groupBy(*keys).count().filter(F.col('count') > 1),
                  f'{label}: conflicting business records for the same key')
    return unique


def build_gold(spark, inputs, start, end):
    from pyspark.sql import functions as F, Window
    dates_between(start, end)
    claims = deduplicate(inputs['claims_events'], CLAIM_FIELDS,
                         ['claim_id', 'source_version'], 'claims')
    require_empty(claims.filter(
        F.col('claim_id').isNull() | F.col('policy_id').isNull() |
        F.col('source_version').isNull() | (F.col('source_version') <= 0) |
        F.col('incident_date').isNull() | F.col('reported_date').isNull() |
        (F.col('reported_date') < F.col('incident_date')) |
        (F.col('reported_date') > F.lit(end).cast('date')) |
        F.col('source_updated_at').isNull() |
        (F.to_date('source_updated_at') > F.lit(end).cast('date')) |
        F.col('incurred_amount').isNull() | (F.col('incurred_amount') < 0) |
        F.col('paid_amount').isNull() | (F.col('paid_amount') < 0)), 'Invalid claim economics')
    require_empty(claims.groupBy('claim_id').agg(F.countDistinct('policy_id').alias('n')).filter('n > 1'),
                  'Claim changes policy across versions')
    latest = (claims.withColumn('_rank', F.row_number().over(
        Window.partitionBy('claim_id').orderBy(F.desc('source_version'), F.desc('source_updated_at'))))
        .filter('_rank = 1').drop('_rank'))
    # Current synthetic history has INSERT/UPDATE only. Deletes require an explicit
    # reversal model and must not silently remove claims whose cash still exists.
    require_empty(latest.filter(~F.col('operation').isin('INSERT','UPDATE') | F.col('operation').isNull()),
                  'Unsupported claim operation; resolve deletes before reporting')
    exposure = deduplicate(inputs['daily_exposure'], EXPOSURE_FIELDS,
                           ['policy_id','load_date'], 'exposure policy-day')
    require_empty(exposure.filter(F.col('policy_id').isNull() | F.col('earned_exposure').isNull() |
        (F.col('earned_exposure') < 0) | F.col('earned_premium').isNull() |
        (F.col('earned_premium') < 0)), 'Invalid exposure')
    payments = deduplicate(inputs['claim_payments'], PAYMENT_FIELDS, ['payment_id'], 'payments')
    expenses = deduplicate(inputs['expense_transactions'], EXPENSE_FIELDS, ['expense_id'], 'expenses')
    for label, frame, id_col, date_col in [('payments',payments,'payment_id','payment_date'),
                                          ('expenses',expenses,'expense_id','expense_date')]:
        require_empty(frame.filter(F.col(id_col).isNull() | F.col('amount').isNull() |
            (F.col('amount') <= 0) | F.col('currency').isNull() | (F.col('currency') != 'CAD') |
            F.col(date_col).isNull() | (F.col(date_col) > F.lit(end).cast('date'))),
            f'Invalid {label}; Gold v1 supports CAD only')
    require_empty(expenses.filter(
        F.col('expense_scope').isNull() | ~F.col('expense_scope').isin('CLAIM','POLICY','PORTFOLIO','CORPORATE') |
        ((F.col('expense_scope') == 'CLAIM') & (F.col('claim_id').isNull() | F.col('policy_id').isNull())) |
        ((F.col('expense_scope') == 'POLICY') & (F.col('policy_id').isNull() | F.col('claim_id').isNotNull())) |
        (F.col('expense_scope').isin('PORTFOLIO','CORPORATE') &
            (F.col('claim_id').isNotNull() | F.col('policy_id').isNotNull())) |
        ((F.col('expense_scope') == 'CORPORATE') & F.col('region').isNotNull())),
        'Expense scope/identifier allocation violation')
    for name, df in [('c',latest),('x',exposure),('p',payments),('e',expenses)]:
        df.createOrReplaceTempView(name)
    require_empty(spark.sql('''SELECT p.payment_id FROM p LEFT JOIN c
        ON p.claim_id=c.claim_id AND p.policy_id=c.policy_id
        WHERE c.claim_id IS NULL OR p.payment_date < c.reported_date
        OR p.payment_date < c.incident_date'''), 'Payment claim/policy/date mismatch')
    require_empty(spark.sql('''SELECT e.expense_id FROM e LEFT JOIN c
        ON e.claim_id=c.claim_id AND e.policy_id=c.policy_id
        WHERE e.expense_scope='CLAIM' AND
        (c.claim_id IS NULL OR e.expense_date<c.reported_date)'''), 'Claim expense relationship mismatch')

    claim_snapshot = spark.sql('''
        SELECT c.*, COALESCE(p.payment_count,0) AS payment_count,
          CAST(COALESCE(p.ledger_paid,0) AS DECIMAL(28,4)) AS ledger_paid_amount,
          CAST(c.paid_amount-COALESCE(p.ledger_paid,0) AS DECIMAL(28,4)) AS paid_reconciliation_difference,
          c.paid_amount=COALESCE(p.ledger_paid,0) AS paid_reconciled,
          CAST(c.incurred_amount-c.paid_amount AS DECIMAL(28,4)) AS case_outstanding_amount,
          c.paid_amount>c.incurred_amount AS negative_case_outstanding
        FROM c LEFT JOIN (SELECT claim_id,COUNT(*) payment_count,SUM(amount) ledger_paid
                          FROM p GROUP BY claim_id) p ON c.claim_id=p.claim_id
    ''')
    claim_snapshot.createOrReplaceTempView('claim_snapshot_v')
    # Only accident dates inside the exposure reporting window contribute losses.
    # Paid cash is a reconciliation measure, never deducted again from incurred.
    policy = spark.sql(f'''
      WITH premiums AS (
        SELECT policy_id,SUM(earned_premium) earned_premium,SUM(earned_exposure) earned_exposure
        FROM x GROUP BY policy_id),
      losses AS (
        SELECT policy_id,COUNT(*) reported_claim_count,SUM(incurred_amount) reported_incurred,
          SUM(paid_amount) source_paid,SUM(ledger_paid_amount) ledger_paid,
          SUM(CASE WHEN NOT paid_reconciled THEN 1 ELSE 0 END) unreconciled_claim_count,
          SUM(CASE WHEN negative_case_outstanding THEN 1 ELSE 0 END) negative_reserve_claim_count
        FROM claim_snapshot_v WHERE incident_date BETWEEN DATE '{start}' AND DATE '{end}'
        GROUP BY policy_id),
      costs AS (
        SELECT policy_id,
          SUM(CASE WHEN expense_scope='CLAIM' THEN amount ELSE 0 END) claim_expense,
          SUM(CASE WHEN expense_scope='POLICY' THEN amount ELSE 0 END) policy_expense
        FROM e WHERE expense_scope IN ('CLAIM','POLICY')
          AND expense_date BETWEEN DATE '{start}' AND DATE '{end}' GROUP BY policy_id),
      attributes AS (
        SELECT *,ROW_NUMBER() OVER(PARTITION BY policy_id ORDER BY load_date DESC) rn FROM x),
      ids AS (SELECT policy_id FROM premiums UNION SELECT policy_id FROM losses UNION SELECT policy_id FROM costs)
      SELECT ids.policy_id,COALESCE(a.region,'UNKNOWN') region,
        COALESCE(a.customer_segment,'UNKNOWN') customer_segment,COALESCE(a.age_band,'UNKNOWN') age_band,
        a.policy_id IS NULL AS missing_exposure_attributes,
        CAST(COALESCE(p.earned_premium,0) AS DECIMAL(28,4)) earned_premium,
        CAST(COALESCE(p.earned_exposure,0) AS DECIMAL(28,8)) earned_exposure,
        COALESCE(l.reported_claim_count,0) reported_claim_count,
        CAST(COALESCE(l.reported_incurred,0) AS DECIMAL(28,4)) reported_incurred_amount,
        CAST(COALESCE(l.source_paid,0) AS DECIMAL(28,4)) source_paid_amount,
        CAST(COALESCE(l.ledger_paid,0) AS DECIMAL(28,4)) ledger_paid_amount,
        CAST(COALESCE(c.claim_expense,0) AS DECIMAL(28,4)) direct_claim_expense,
        CAST(COALESCE(c.policy_expense,0) AS DECIMAL(28,4)) direct_policy_expense,
        COALESCE(l.unreconciled_claim_count,0) unreconciled_claim_count,
        COALESCE(l.negative_reserve_claim_count,0) negative_reserve_claim_count
      FROM ids LEFT JOIN premiums p ON ids.policy_id=p.policy_id
        LEFT JOIN losses l ON ids.policy_id=l.policy_id
        LEFT JOIN costs c ON ids.policy_id=c.policy_id
        LEFT JOIN attributes a ON ids.policy_id=a.policy_id AND a.rn=1
    ''')
    policy = (policy.withColumn('direct_expense',F.col('direct_claim_expense')+F.col('direct_policy_expense'))
        .withColumn('reported_contribution_before_ibnr',
            F.col('earned_premium')-F.col('reported_incurred_amount')-F.col('direct_expense')))
    for alias, numerator, denominator in [
        ('reported_loss_ratio','reported_incurred_amount','earned_premium'),
        ('reported_claim_frequency','reported_claim_count','earned_exposure'),
        ('average_reported_claim_severity','reported_incurred_amount','reported_claim_count')]:
        policy=policy.withColumn(alias,F.when(F.col(denominator)>0,
            F.col(numerator).cast('double')/F.col(denominator).cast('double')))
    policy.createOrReplaceTempView('policy_v')
    expense_summary = spark.sql(f'''
      SELECT expense_scope,expense_category,region,currency,COUNT(*) transaction_count,
        CAST(SUM(amount) AS DECIMAL(28,4)) expense_amount
      FROM e WHERE expense_date BETWEEN DATE '{start}' AND DATE '{end}'
      GROUP BY expense_scope,expense_category,region,currency
    ''')
    expense_summary.createOrReplaceTempView('expense_summary_v')
    portfolio=spark.sql('''
      SELECT 'ALL' portfolio, 'CAD' currency, COUNT(*) policy_count,
        CAST(COALESCE(SUM(earned_premium),0) AS DECIMAL(28,4)) earned_premium,
        CAST(COALESCE(SUM(earned_exposure),0) AS DECIMAL(28,8)) earned_exposure,
        COALESCE(SUM(reported_claim_count),0) reported_claim_count,
        CAST(COALESCE(SUM(reported_incurred_amount),0) AS DECIMAL(28,4)) reported_incurred_amount,
        CAST(COALESCE(SUM(source_paid_amount),0) AS DECIMAL(28,4)) source_paid_amount,
        CAST(COALESCE(SUM(ledger_paid_amount),0) AS DECIMAL(28,4)) ledger_paid_amount,
        CAST(COALESCE(SUM(direct_expense),0) AS DECIMAL(28,4)) direct_expense,
        COALESCE(SUM(unreconciled_claim_count),0) unreconciled_claim_count,
        COALESCE(SUM(negative_reserve_claim_count),0) negative_reserve_claim_count,
        COALESCE(SUM(CASE WHEN missing_exposure_attributes THEN 1 ELSE 0 END),0) policies_missing_exposure
      FROM policy_v
    ''')
    overhead=spark.sql('''SELECT
      CAST(COALESCE(SUM(CASE WHEN expense_scope='PORTFOLIO' THEN expense_amount ELSE 0 END),0) AS DECIMAL(28,4)) portfolio_expense,
      CAST(COALESCE(SUM(CASE WHEN expense_scope='CORPORATE' THEN expense_amount ELSE 0 END),0) AS DECIMAL(28,4)) corporate_expense
      FROM expense_summary_v''')
    portfolio=(portfolio.crossJoin(overhead)
        .withColumn('total_expense',F.col('direct_expense')+F.col('portfolio_expense')+F.col('corporate_expense'))
        .withColumn('reported_result_before_ibnr',F.col('earned_premium')-F.col('reported_incurred_amount')-F.col('total_expense'))
        .withColumn('reported_loss_ratio',F.when(F.col('earned_premium')>0,F.col('reported_incurred_amount')/F.col('earned_premium')))
        .withColumn('expense_ratio',F.when(F.col('earned_premium')>0,F.col('total_expense')/F.col('earned_premium')))
        .withColumn('reported_combined_ratio',F.when(F.col('earned_premium')>0,
            (F.col('reported_incurred_amount')+F.col('total_expense'))/F.col('earned_premium'))))
    result=dict(zip(TABLES,[claim_snapshot,policy,portfolio,expense_summary]))
    return {name:df.withColumn('period_start',F.lit(start).cast('date'))
            .withColumn('period_end',F.lit(end).cast('date'))
            .withColumn('gold_processed_at',F.current_timestamp()) for name,df in result.items()}


def read_inputs(spark, s3, start, end):
    required=set(dates_between(start,end))
    inputs={}
    for name in DATASETS:
        available=set()
        for page in s3.get_paginator('list_objects_v2').paginate(Bucket='silver-auto-insurance',Prefix=name+'/'):
            for obj in page.get('Contents',[]):
                parts=obj['Key'].split('/')
                if len(parts)>=3 and parts[1].startswith('load_date=') and parts[-1].endswith('.parquet'):
                    available.add(parts[1].split('=',1)[1])
        missing=sorted(required-available)
        if missing:
            raise ValueError(f'{name}: missing Silver Parquet partitions: {missing}. Run Bronze-to-Silver first.')
        root=f's3://silver-auto-insurance/{name}/'
        inputs[name]=spark.read.option('basePath',root).parquet(*[root+'load_date='+d+'/' for d in sorted(required)])
    return inputs


def publish(glue, s3, outputs, start, end, run_id):
    # Unique generations avoid partial replacement of live data. Existing date
    # partitions are pointed to the new generation after all files are written.
    # Catalog updates across four tables are NOT atomic: the final manifest and
    # successful Glue run are the publication barrier for downstream consumers.
    from pyspark.sql import functions as F
    try:glue.create_database(DatabaseInput={'Name':DATABASE})
    except glue.exceptions.AlreadyExistsException:pass
    plans={}
    for name,df in outputs.items():
        columns=[{'Name':field.name,'Type':field.dataType.simpleString()} for field in df.schema.fields]
        root=f's3://gold-auto-insurance/{name}/'
        descriptor={'Columns':columns,'Location':root,
            'InputFormat':'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat':'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo':{'SerializationLibrary':'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'}}
        table={'Name':name,'TableType':'EXTERNAL_TABLE','Parameters':{'classification':'parquet','EXTERNAL':'TRUE'},
            'StorageDescriptor':descriptor,'PartitionKeys':[{'Name':'as_of_date','Type':'string'}]}
        try:
            old=glue.get_table(DatabaseName=DATABASE,Name=name)['Table']
            if old['StorageDescriptor']['Columns'] != columns or old.get('PartitionKeys') != table['PartitionKeys']:
                raise ValueError(f'Gold schema mismatch for {name}; explicit schema migration required')
            if old['StorageDescriptor']['Location'].rstrip('/') != root.rstrip('/'):
                raise ValueError(f'Unexpected Gold location for {name}')
        except glue.exceptions.EntityNotFoundException:
            glue.create_table(DatabaseName=DATABASE,TableInput=table)
        location=f's3://gold-auto-insurance/_versions/{name}/as_of_date={end}/run_id={run_id}/'
        plans[name]=(df,descriptor,location)
    counts={}
    for name,(df,descriptor,location) in plans.items():
        counts[name]=df.count()
        df.coalesce(1).write.mode('errorifexists').parquet(location)
    for name,(df,descriptor,location) in plans.items():
        descriptor=dict(descriptor,Location=location)
        partition={'Values':[end],'StorageDescriptor':descriptor,'Parameters':{'gold_run_id':run_id,'period_start':start}}
        try:glue.create_partition(DatabaseName=DATABASE,TableName=name,PartitionInput=partition)
        except glue.exceptions.AlreadyExistsException:
            glue.update_partition(DatabaseName=DATABASE,TableName=name,PartitionValueList=[end],PartitionInput=partition)
    manifest={'run_id':run_id,'period_start':start,'as_of_date':end,'rows':counts,
              'locations':{name:v[2] for name,v in plans.items()}}
    s3.put_object(Bucket='gold-auto-insurance',Key=f'_manifests/as_of_date={end}/latest.json',
                  Body=json.dumps(manifest).encode(),ContentType='application/json')
    print(json.dumps(manifest))


def main():
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    import boto3
    args=getResolvedOptions(sys.argv,['JOB_NAME','PROCESS_DATE','HISTORY_START_DATE','TEST_MODE'])
    start,end=args['HISTORY_START_DATE'],args['PROCESS_DATE']
    dates_between(start,end)
    if args['TEST_MODE'] not in ('true','false'):
        raise ValueError('TEST_MODE must be true or false')
    context=GlueContext(SparkContext.getOrCreate());spark=context.spark_session
    spark.conf.set('spark.sql.session.timeZone','UTC')
    spark.conf.set('spark.sql.shuffle.partitions','8')
    spark.conf.set('spark.sql.parquet.compression.codec','snappy')
    job=Job(context);job.init(args['JOB_NAME'],args)
    if args['TEST_MODE']=='true':
        self_test(spark)
    else:
        s3=boto3.client('s3');glue=boto3.client('glue')
        inputs=read_inputs(spark,s3,start,end)
        outputs=build_gold(spark,inputs,start,end)
        publish(glue,s3,outputs,start,end,uuid.uuid4().hex)
    job.commit()


def self_test(spark):
    """Real Spark integration tests; no S3 or catalog writes."""
    from pyspark.sql import functions as F
    def frame(rows, columns):
        return spark.createDataFrame(rows, ','.join(k+' string' for k in columns))
    claims=frame([
        ['CLM-1','POL-1','1','2026-01-01T06:00:00Z','2026-01-01','2026-01-01','INSERT','OPEN','100','0'],
        ['CLM-1','POL-1','2','2026-01-02T06:00:00Z','2026-01-01','2026-01-01','UPDATE','CLOSED','120','100'],
        ['CLM-2','POL-3','1','2026-01-01T06:00:00Z','2026-01-01','2026-01-01','INSERT','OPEN','10','0'],
    ],CLAIM_FIELDS)
    for col in ['source_version']:claims=claims.withColumn(col,F.col(col).cast('int'))
    for col in ['paid_amount','incurred_amount']:claims=claims.withColumn(col,F.col(col).cast('decimal(18,2)'))
    for col in ['incident_date','reported_date']:claims=claims.withColumn(col,F.col(col).cast('date'))
    claims=claims.withColumn('source_updated_at',F.col('source_updated_at').cast('timestamp'))
    exposure=frame([['POL-1','2026-01-01','Ontario','Standard','35-49','0.01','100'],
                    ['POL-1','2026-01-02','Ontario','Standard','35-49','0.01','100'],
                    ['POL-2','2026-01-01','Ontario','Standard','35-49','0.01','100']],EXPOSURE_FIELDS)
    for col in ['earned_premium','earned_exposure']:exposure=exposure.withColumn(col,F.col(col).cast('decimal(18,8)'))
    payments=frame([['PAY-1','CLM-1','POL-1','2026-01-02','INDEMNITY','CLAIMANT','60','CAD','EVT-2','2','TEST','2026-01-02','2026-01-02'],
                    ['PAY-2','CLM-1','POL-1','2026-01-02','INDEMNITY','CLAIMANT','40','CAD','EVT-2','2','TEST','2026-01-02','2026-01-02']],PAYMENT_FIELDS)
    payments=payments.withColumn('amount',F.col('amount').cast('decimal(18,2)')).withColumn('payment_date',F.col('payment_date').cast('date'))
    expenses=frame([['E1','2026-01-01','CLAIM_ADJUSTING','CLAIM','5','CAD','CLM-1','POL-1','Ontario','TEST','2026-01-01','2026-01-01'],
                    ['E2','2026-01-01','BROKER_COMMISSION','POLICY','15','CAD',None,'POL-1','Ontario','TEST','2026-01-01','2026-01-01'],
                    ['E3','2026-01-01','OFFICE_RENT','CORPORATE','30','CAD',None,None,None,'TEST','2026-01-01','2026-01-01'],
                    ['E4','2026-01-01','REGIONAL_MARKETING','PORTFOLIO','20','CAD',None,None,'Ontario','TEST','2026-01-01','2026-01-01']],EXPENSE_FIELDS)
    expenses=expenses.withColumn('amount',F.col('amount').cast('decimal(18,2)')).withColumn('expense_date',F.col('expense_date').cast('date'))
    inputs=dict(zip(DATASETS,[claims,exposure,payments.unionByName(payments),expenses]))
    outputs=build_gold(spark,inputs,'2026-01-01','2026-01-02')
    p={r['policy_id']:r.asDict() for r in outputs['policy_performance'].collect()}
    assert p['POL-1']['earned_premium']==200 and p['POL-1']['reported_incurred_amount']==120
    assert p['POL-1']['reported_contribution_before_ibnr']==60 # 200-120-20; do not subtract cash again
    assert p['POL-2']['reported_claim_count']==0
    assert p['POL-3']['reported_loss_ratio'] is None and p['POL-3']['missing_exposure_attributes']
    total=outputs['portfolio_performance'].first()
    assert total['earned_premium']==300 and total['reported_incurred_amount']==130
    assert total['total_expense']==70 and total['reported_result_before_ibnr']==100
    assert total['ledger_paid_amount']==100 and total['unreconciled_claim_count']==0
    assert outputs['claim_snapshot'].count()==2
    assert outputs['expense_summary'].count()==4
    # Empty payment history must still produce a report and expose reconciliation differences.
    empty=dict(inputs,claim_payments=payments.limit(0))
    assert build_gold(spark,empty,'2026-01-01','2026-01-02')['portfolio_performance'].first()['unreconciled_claim_count']==1
    # Cross-partition conflicting transaction IDs must fail, not select a random amount.
    conflict=payments.unionByName(payments.withColumn('amount',F.lit(99).cast('decimal(18,2)')))
    try:build_gold(spark,dict(inputs,claim_payments=conflict),'2026-01-01','2026-01-02')
    except ValueError as error:assert 'conflicting business' in str(error)
    else:raise AssertionError('Conflicting payment IDs were accepted')
    print('GOLD_SELF_TEST_PASS: aggregation, latest claim, duplicates, overhead, empty payments, zero denominator, missing exposure, conflicts')


if __name__=='__main__':
    main()
