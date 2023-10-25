// Databricks notebook source
import spark.implicits._

elig_ban_cond_parsed_list = elig_ban_cond.trim()
    .split("\n")
    .map(_.trim)
    .mkString("\n")
    .split("\n")
    .map(x => x.split(","))
    .map(x => (x(0), x(1), x(2), x(3), x(4)))
    .toList

elig_ban_cond_col_names = List("mob_srv", "internet_srv", "voice_srv", "elig_ban", "package_type")

DF_elig_ban_cond_check = Seq(elig_ban_cond_parsed_list: _*).toDF(elig_ban_cond_col_names: _*)

DF_elig_ban_before_check = productTranslationDF.withColumn("mob", when(col("prod_id_srv") === "1-W35PL", 1).otherwise(0))
    .withColumn("internet", when(col("prod_id_srv") === "1-1ACC6", 1).otherwise(0))
    .withColumn("voice", when(col("prod_id_srv") === "1-1HZX1", 1).otherwise(0))
    .groupBy("ban")
    .agg(
        sum("mob").as("mob_srv")
        , sum("internet").as("internet_srv")
        , sum("voice").as("voice_srv")
        )
    .withColumn("new_mob_srv", when(col("mob_srv") > 1, lit(">1")).when(col("mob_srv") === 1, lit("1")).otherwise(col("mob_srv")))
    .withColumn("new_internet_srv", when(col("internet_srv") > 1, lit(">1")).when(col("internet_srv") === 1, lit("1")).otherwise(col("internet_srv")))
    .withColumn("new_voice_srv", when(col("voice_srv") > 1, lit(">1")).when(col("voice_srv") === 1, lit("1")).otherwise(col("voice_srv")))

elig_ban_df = DF_elig_ban_before_check.alias("a").join(
    DF_elig_ban_cond_check.alias("b")
    , DF_elig_ban_before_check("new_mob_srv") === DF_elig_ban_cond_check("mob_srv") &&
      DF_elig_ban_before_check("new_internet_srv") === DF_elig_ban_cond_check("internet_srv") &&
      DF_elig_ban_before_check("new_voice_srv") === DF_elig_ban_cond_check("voice_srv")
    , "left"
    ).select("a.ban", "a.mob_srv", "a.internet_srv", "a.voice_srv", "b.elig_ban", "b.package_type")

serviceCountDF = workflowManager.dataFrameMap(Constants.PRODUCT_TRANSLATION_DF)._1
    .filter(col("bundle_products") =!= 'Y')
    .select(col("ban") as "ban_count", col("asset_row_id_srv"))
    .groupBy("ban_count")
    .agg(countDistinct(col("asset_row_id_srv")).alias("service_count"))

notification_period_df = workflowManager.dataFrameMap(Constants.PRODUCT_TRANSLATION_DF)._1
    .groupBy(prospesctiveGroupByCol.split(",").map(c => col(c)): _*)
    .agg(
        max(col("ban")).alias("ban_max"), //alias changed
        max(Constants.NOTIFICATION_PERIOD).alias(Constants.NOTIFICATION_PERIOD),
        max(col("additional_comms_required")).alias("additional_comms_required")
        )

joined_df = productTranslationDF.join(notification_period_df, Seq("ban", "ban"), "left")
    .join(serviceCountDF, col("ban_count") === col("ban"), "inner")
    .drop("ban_count")

/** DEDPT-6729 Modification Ends here *** */
