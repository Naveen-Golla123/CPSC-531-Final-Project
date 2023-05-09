from pyspark.sql.functions import *
from pyspark.sql.functions import count, avg
from pyspark.sql.functions import col
# import matplotlib.pyplot as plt
import seaborn as sns
# import numpy as np
from pyspark.sql import SparkSession
import paramiko


# Database configuration
postgresql_host_name = "baasu.db.elephantsql.com"
postgresql_port_no = "5432"
postgresql_user_name = "pbomcruv"
postgresql_password = "6YqMz3n1BfiIZCnaFBTyXiURB6OQCGp9"
postgresql_database_name = "pbomcruv"
postgresql_driver = "org.postgresql.Driver"
postgresql_jdbc_url = "jdbc:postgresql://baasu.db.elephantsql.com:5432/pbomcruv"
    

db_properties = {'user': postgresql_user_name, 'password': postgresql_password, 'driver': postgresql_driver}

# Spark config Constants
SPARK_APP_NAME = "Server data analysis"
sns.set()

# SSH connection to EC2 instance.
# ssh = paramiko.SSHClient()
# ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
# ssh.connect(hostname='ec2-34-239-177-215.compute-1.amazonaws.com', username='ubuntu',
#             key_filename='/Users/csuftitan/Downloads/Hadoop_Multinode_Cluster.pem')
# sftp = ssh.open_sftp()

def save_dataframe_to_sql(current_df, postgresql_table_name):

    # current_df.write.csv(f'output/{postgresql_table_name}.csv', header=True)
    # print("Printing postgresql_table_name: " + postgresql_table_name)

    #Save the dataframe to the table.
    current_df.write.option("truncate", True).jdbc(url=postgresql_jdbc_url,table = postgresql_table_name,
                  mode = 'overwrite',
                  properties = db_properties)

if __name__ == "__main__":
    # Start Spark Session
    spark = SparkSession.builder.appName(SPARK_APP_NAME).master("local[*]").getOrCreate()

    spark.sparkContext.addFile('./postgresql-42.6.0.jar')

    # config("spark.hadoop.fs.defaultFS","hdfs://ec2-34-239-177-215.compute-1.amazonaws.com:9000").\
    # Read Spark Session.

    # matches = 'matches.csv'
    # sftp.get('/home/ubuntu/IPL_Matches_2008_2020.csv', matches)
    # ball_to_ball_temp = 'ball_to_ball.csv'
    # sftp.get('/home/ubuntu/IPL_Ball_by_Ball_2008_2020.csv', ball_to_ball_temp)



    # # Load data from HDFS
    # matches_df = spark.read.format("csv").option("header", "true").load("hdfs://ec2-34-239-177-215.compute-1.amazonaws.com:9000/namenode/data/IPL_Ball_by_Ball_2008_2020.csv")
    # ball_to_ball_df = spark.read.format("csv").option("header", "true").load(ball_to_ball_temp)

    matches_df = spark.read.format("csv").option("header", "true").load("s3://cpsc-531-input-bucket/input_folder/IPL_Matches_2008_2020.csv")
    ball_to_ball_df = spark.read.format("csv").option("header", "true").load("s3://cpsc-531-input-bucket/input_folder/IPL_Ball_by_Ball_2008_2020.csv")

    # Merging two datasets
    merged_df = matches_df.join(ball_to_ball_df, "id")
    merged_df.show()

    # Generating year column
    merged_df = merged_df.withColumn("Year", substring(col("Date"), 0, 4))
    merged_df = merged_df.sort("Year")
    merged_df.show()
    
    # DATA CLEANING - Replace deccan Chargers with Sun Risers
    replaceCol_df = merged_df
    for column in ["batting_team", "bowling_team", "team1", "team2", "toss_winner", "winner"]:
        replaceCol_df = replaceCol_df.withColumn(column,
                                                 when(col(column) == "Deccan Chargers",
                                                      "Sunrisers Hyderabad").otherwise(
                                                     col(column)))
        replaceCol_df = replaceCol_df.withColumn(column,
                                                 when(col(column) == "Delhi Daredevils",
                                                      "Delhi Capitals").otherwise(
                                                     col(column)))
        replaceCol_df = replaceCol_df.withColumn(column,
                                                 when(col(column) == "Rising Pune Supergiant",
                                                      "Rising Pune Supergiants").otherwise(
                                                     col(column)))
    cleaned_df = replaceCol_df
    # cleaned_df.show()
    '''
        Players who made highest number of Sixes and fours.
    '''
    highest_Number_of_4and6s = merged_df.groupBy("batsman").agg(
        count(when(col("batsman_runs") == 4, True)).alias("Number_of_Fours"),
        count(when(col("batsman_runs") == 6, True)).alias("Number_of_Sixes"),
        last("batting_team").alias("Most_recent_team"))
    highest_Number_of_4and6s = highest_Number_of_4and6s.withColumn("Total_Boundaries",
                                                                   col("Number_of_Fours") + col("Number_of_Sixes"))
    highest_Number_of_4and6s = highest_Number_of_4and6s.sort('Total_Boundaries', ascending=False)
    highest_Number_of_4and6s = highest_Number_of_4and6s.limit(20)
    save_dataframe_to_sql(highest_Number_of_4and6s,"highest_Number_of_4and6s")
    
    '''
        Win factor of team by player score. ( histogram )
    '''
    
    
    """
        ecomony rate
    """
    
    '''
        Home ground
    '''
    
    '''
        Pie chart for extra runs
    '''
    extra_runs_df = cleaned_df.groupBy("extras_type").agg(sum("extra_runs").alias("Extra_Runs"))
    extra_runs_df = extra_runs_df.filter(col("extras_type") != "NA")
    save_dataframe_to_sql(extra_runs_df,"ExtraRuns")
    # extra_runs_df.show()
    
    extra_runs_pandas_df = extra_runs_df.toPandas()
    # plt.pie(extra_runs_pandas_df["Extra_Runs"], labels=extra_runs_pandas_df["extras_type"], autopct="%1.1f%%")
    # plt.title("Extra Runs")
    # plt.show()
    #
    '''
        Highest wicket taker and what kinds
    '''
    
    '''
        Team wins in chasing/Defending.
    '''
    inningsWin_df = cleaned_df.groupBy("id", "winner", "result").agg(count("*").alias("Dummy"))
    inningsWin_df = inningsWin_df.groupBy("winner", "result").agg(count("*").alias("Matchs_win_count"))
    inningsWin_df = inningsWin_df.filter((col("result") != "tie") & (col("result") != "NA"))
    pivoted_innings_df = inningsWin_df.groupBy("winner").pivot("result").sum("Matchs_win_count")
    save_dataframe_to_sql(pivoted_innings_df, "inningsWin_df")
    pdf = pivoted_innings_df.toPandas()
    # fig, ax = plt.subplots(figsize=(12, 8))
    # bar_width = 0.35
    # opacity = 0.8
    #
    # runs = pdf["runs"]
    # wickets = pdf["wickets"]
    # teams = pdf["winner"]
    # x_pos = np.arange(len(teams))
    
    # ax.bar(x_pos, runs, bar_width, alpha=opacity, color='b', label='Defending')
    # ax.bar(x_pos + bar_width, wickets, bar_width, alpha=opacity, color='g', label='Chasing')
    #
    # ax.set_xlabel('Team')
    # ax.set_ylabel('Won Matches')
    # ax.set_title('Team Performance')
    # ax.set_xticks(x_pos + bar_width / 2)
    # ax.set_xticklabels(teams, rotation=20, ha="right")
    #
    # ax.legend()
    # plt.show()
    
    '''
        Year vs Player( or team ) and count line graph // Too many lines
    '''
    line_graph_scores = cleaned_df.groupBy("year", "batting_team").agg(sum("total_runs").alias("Total_Runs"))
    pivoted_df = line_graph_scores.groupBy("year").pivot("batting_team").sum("Total_Runs")
    
    # pivoted_df.show()
    # pivoted_df = pivoted_df.withColumn("Delhi Capitals", when(col("Delhi Capitals").isNull(), col("Delhi Daredevils")).otherwise(col("Delhi Capitals")))
    pivoted_df = pivoted_df.drop('Gujarat Lions', 'Kochi Tuskers Kerala', 'Pune Warriors', 'Rising Pune Supergiant',
                                 'Rajasthan Royals', 'Rising Pune Supergiant', 'Rising Pune Supergiants')
    # pivoted_df.show()
    save_dataframe_to_sql(pivoted_df, "line_graph_scores")
    
    pandas_df = pivoted_df.toPandas()
    x_column = 'year'
    y_columns = ['Chennai Super Kings', 'Delhi Capitals', 'Kings XI Punjab', 'Kolkata Knight Riders', 'Mumbai Indians',
                 'Royal Challengers Bangalore', 'Sunrisers Hyderabad']
    
    # Define colors and line styles for each series
    colors = ['blue', 'red', 'green']
    linestyles = ['-', '--', ':']
    
    # Create a line plot of multiple series
    # fig, ax = plt.subplots()
    for i, colmn in enumerate(y_columns):
        ax.plot(pandas_df[x_column], pandas_df[colmn], label=colmn)
    ax.legend()
    # plt.show()
    
    '''
        Population Pyramid reference -> https://datavizcatalogue.com/methods/population_pyramid.html
    '''
    cleaned_df = cleaned_df.withColumn("over", col('over').cast('integer'))
    score_per_over_df = cleaned_df.groupBy("id", "inning", "over", "batting_team").agg(
        sum("total_runs").alias("runs_per_over"))
    over_range_innings = score_per_over_df.withColumn("over",
                                                      when(col("over") <= 5, "Batting PowerPlay 1").when(
                                                          col("over") >= 16,
                                                          "Batting Powerplay 2").otherwise(
                                                          "Bowling Powerplay")).alias("Overs Range")
    
    over_range_innings = over_range_innings.groupBy("batting_team", "over").agg(avg("runs_per_over").alias("Avg_Runs"))
    
    # Rename team names to acronym
    over_range_innings = over_range_innings.withColumn("batting_team",
                                                       when(col("batting_team") == "Sunrisers Hyderabad", "SRH").
                                                       when(col("batting_team") == "Chennai Super Kings", "CSK").
                                                       when(col("batting_team") == "Kochi Tuskers Kerala", "KTK").
                                                       when(col("batting_team") == "Rajasthan Royals", "RR").
                                                       when(col("batting_team") == "Gujarat Lions", "GL").
                                                       when(col("batting_team") == "Kings XI Punjab", "KXIP").
                                                       when(col("batting_team") == "Pune Warriors", "PW").
                                                       when(col("batting_team") == "Delhi Capitals", "DC").
                                                       when(col("batting_team") == "Mumbai Indians", "MI").
                                                       when(col("batting_team") == "Kolkata Knight Riders", "KKR").
                                                       when(col("batting_team") == "Royal Challengers Bangalore",
                                                            "RCB").
                                                       when(col("batting_team") == "Rising Pune Supergiants", "RPS"))
    
    save_dataframe_to_sql(over_range_innings,"over_range_innings")
    
    over_range_innings = over_range_innings.groupBy("batting_team").pivot("over").sum("Avg_Runs")
    over_range_innings_df = over_range_innings.toPandas()
    fig, axes = plt.subplots(1, 3, figsize=(20, 10), sharey=True)
    fig.suptitle('Teams score in powerplays')
    batting_power_play1 = over_range_innings_df["Batting PowerPlay 1"]
    bowling_power_play = over_range_innings_df["Bowling Powerplay"]
    batting_power_play2 = over_range_innings_df["Batting Powerplay 2"]
    batting_team_plt = over_range_innings_df["batting_team"]
    
    # Batting Powerplay 1
    sns.barplot(ax=axes[0], x=batting_team_plt.values, y=batting_power_play1.values, width=1)
    axes[0].set_title(batting_power_play1.name)
    axes[0].set_xticklabels(batting_team_plt.values, rotation=20, ha="right")
    #
    # Bowling power play
    sns.barplot(ax=axes[1], x=batting_team_plt.values, y=bowling_power_play.values, width=1)
    axes[1].set_title(bowling_power_play.name)
    axes[1].set_xticklabels(batting_team_plt.values, rotation=20, ha="right")
    #
    # Batting power play 2
    sns.barplot(ax=axes[2], x=batting_team_plt.values, y=batting_power_play2.values, width=1)
    axes[2].set_title(batting_power_play2.name)
    axes[2].set_xticklabels(batting_team_plt.values, rotation=20, ha="right")
    
    # plt.show()
    over_range_innings.show()
    '''
    
    '''
    
    while True:
        continue
        #
        #  boundary oercentagex
        # dots percentage
        # toss as factor
        # Power play
