package league

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{broadcast, col, count, desc, lit, sum, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}


object classification extends App {

  // input parameters
  val season = "2011/2012"
  val country = "Spain"
  val leagueId = "21518"

  // create local spark session
  val spark = SparkSession.builder
    .appName("classfication")
    .master("local[*]")
    .getOrCreate()

  // changing log level
  spark.sparkContext.setLogLevel("ERROR")

  // load matches file

  import spark.implicits._
  val df_match_raw = spark.read
    .format("csv").option("header", "true")
    .load("src/main/resources/match.csv.bz2")

  // selecting, filtering for given country, league and season
  val df_match = df_match_raw.select(
      'country_id, 'league_id, 'season, 'home_team_api_id, 'away_team_api_id, 'home_team_goal, 'away_team_goal)

  val df_match_season = filterBySeason(df_match, season)
  val df_match_season_league = filterByLeagueId(df_match_season, leagueId)
  val df_match_season_league_country = filterByCountry(df_match_season_league, country)

  // show
  df_match_season_league_country.show(20, truncate = false)

  // creating udf function
  val calculateMatchPointsUDF = createCalculateMatchPointsUDF()

  val df_base = df_match_season_league_country
    .withColumn("home_team_points", calculateMatchPointsUDF(col("home_team_goal"), col("away_team_goal")))
    .withColumn("away_team_points", calculateMatchPointsUDF(col("away_team_goal"), col("home_team_goal")))
    .drop("country_id", "league_id", "season")
    .cache()

  // creating two separated dataframes at team level for both home and away matches
  val df_home_team = df_base.select(
    col("home_team_api_id").as("team_id"),
    col("home_team_goal").as("goals_for"),
    col("away_team_goal").as("goals_against"),
    col("home_team_points").as("points"))

  val df_away_team = df_base
    .select(
      col("away_team_api_id").as("team_id"),
      col("away_team_goal").as("goals_for"),
      col("home_team_goal").as("goals_against"),
      col("away_team_points").as("points"))

  val df_team_results = df_home_team.unionByName(df_away_team)

  val df_final = getTeamNames(df_team_results)
    .select(
      col("team"),
      col("goals_for"),
      col("goals_against"),
      col("points")
    )

  df_final.filter(col("team") === "Real Madrid CF").show(100, truncate = false)

  // group to get classification
  val df_classification = df_final
    .groupBy("team")
    .agg(
      sum(col("points")).as("total_points"),
      sum(col("goals_for")).cast("integer").as("total_goals_for"),
      sum(col("goals_against")).cast("integer").as("total_goals_against"),
      (sum(col("goals_for")) - sum(col("goals_against"))).cast("integer").as("total_goals_diff"),
      count(lit(1)).as("played")
    )

  df_classification.orderBy(desc("total_points")).show(100, truncate = false)


  def getTeamNames(df: DataFrame): DataFrame = {
    // lookup for team names
    val df_team = spark.read
      .format("csv")
      .option("header", "true")
      .load("src/main/resources/team.csv.bz2")

    df.join(broadcast(df_team), df_team_results.col("team_id") === df_team.col("team_api_id"))
      .select(
        df_team.col("team_long_name").as("team"),
        df.col("*")
      )
  }

  def filterByCountry(df: DataFrame, countryName: String): DataFrame = {
    // loading countries lookup
    val df_countries = spark.read
      .format("csv")
      .option("header", "true")
      .load("src/main/resources/country.csv.bz2")

    df.join(broadcast(df_countries), df.col("country_id") === df_countries.col("id"))
      .filter(df_countries.col("name") === countryName)
      .select(df.col("*"))
  }

  def filterByLeagueId(df: DataFrame, leagueId: String): DataFrame = {
    df.filter(col("league_id") === leagueId)
  }

  def filterBySeason(df: DataFrame, season: String): DataFrame = {
    df.filter(col("season") === season)
  }

  def createSparkSession(appName: String): SparkSession = {
    SparkSession.builder
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }

  def createCalculateMatchPointsUDF(): UserDefinedFunction = {
    // constants
    val pointsWinner = 3
    val pointsDraw = 1
    val pointsLoser = 0

    // creating UDF to estimate the points
    val calculateMatchPoints = (goalsTeamA: Int, goalsTeamB: Int) => {
      goalsTeamA compare goalsTeamB match {
        case 0 => pointsDraw
        case 1 => pointsWinner
        case -1 => pointsLoser
      }
    }
    udf(calculateMatchPoints)
  }

}