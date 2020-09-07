import league.classification.{createCalculateMatchPointsUDF, createSparkSession}
import org.scalatest.FunSuite
import org.apache.spark.sql.functions.col
import scala.collection.Seq

class classificationTest extends FunSuite {

  test("dummy") {
    assert(1 == 1)
  }

  test("Calculate Match Points") {
    val spark = createSparkSession("test")

    import spark.implicits._
    val df_test = Seq(
      (0, 0),
      (3, 1),
      (2, 5)
    ).toDF("home_team_goal", "away_team_goal")

    val calculateMatchPointsUDF = createCalculateMatchPointsUDF()

    val df_base = df_test
      .withColumn("home_team_points", calculateMatchPointsUDF(col("home_team_goal"), col("away_team_goal")))
      .withColumn("away_team_points", calculateMatchPointsUDF(col("away_team_goal"), col("home_team_goal")))

    val df_match_check = Seq(
      (0, 0, 1, 1),
      (3, 1, 3, 0),
      (2, 5, 0, 3)
    ).toDF("home_team_goal", "away_team_goal","home_team_points", "away_team_points")

    df_test.show(false)
    df_base.show(false)
    df_match_check.show(false)

    assert(df_base.collectAsList().equals(df_match_check.collectAsList()))
  }
}
