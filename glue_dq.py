from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql.functions import F
from datetime import datetime

def dq():
    dldq_rules = """Rules = [
        IsPrimaryKey "id1", "id2"
        IsComplete "id1", "id2", "name"
        IsUnique "id1", "id2"
        IsNotNull "id1", "id2", "name"
        IsInRange "age", 0, 120
    """

    evaluate_dq = EvaluateDataQuality.apply(
        frame=dynf,
        ruleset=dldq_rules
        )
    return evaluate_dq

def create_report(evaluate_dq):
    report = evaluate_dq.toDF() \
        .select("Rule", "Outcome", "FailureReason", "EvaluatedMetrics") \
        .withColumn("anomesdia", F.lit(datetime.now().strftime("%Y%m%d")))
    return report