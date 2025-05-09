# this script contains the code to preprocess de logistics data from all countries

library(tidyverse)
library(readxl)

metadata_dir <- "O:/DGK/IRAS/EEPI/Privacy/Exposome-Panel Study/Datamanagement/data/raw/air_sensors_UL_centra/key_tables"

#### Netherlands
nl_file <- "O:/DGK/IRAS/EEPI/Projects/Exposome-Panel Study/Datamanagement/analysis_data_output/sensor_sending_dates_ULID.csv"

nl_metadata <- read_delim(nl_file, delim = ";") %>%
  mutate(
    # script works with AID so rename ULID to AID
    AID = ULID,
    StartDate = as.Date(sensor_sending_date, format = "%Y-%m-%d"),
    EndDate = as.Date(sensor_return_date, format = "%Y-%m-%d")
  ) %>%
  select(
    AID,
    StaticSensor = static_sensor_IMEI,
    MobileSensor = dynamic_sensor_IMEI,
    StartDate,
    EndDate,
    Measurement = measurement
  )

#### Greece
gr_file <- file.path(metadata_dir, "GR_PMSensors_Summary_20250508.csv")

gr_metadata <- read_delim(gr_file, delim = ";") %>%
  mutate(
    # measurement column is missing so add, all rows are from measurement 1
    Measurement = 1,
    StartDate = as.Date(Sensors.Sending.Date, format = "%Y-%m-%d"),
    EndDate = as.Date(Sensors.Return.Date, format = "%Y-%m-%d")
  ) %>%
  select(
    AID,
    StaticSensor = IMEI.static.air.sensor,
    MobileSensor = IMEI.mobile.air.sensor,
    StartDate,
    EndDate,
    Measurement
  )


#### Switzerland
ch_file <- file.path(metadata_dir, "key_table_SODAQAIR_CH.csv")

ch_metadata <- read_delim(ch_file, delim = ",") %>%
  rename(AID = aid,
         static_sensor_IMEI = part_pm_stat_id_bl,
         dynamic_sensor_IMEI = part_pm_mob_id_bl,
         sensor_sending_date = start_q_date,
         sensor_return_date = end_q_date) %>%
  mutate(
    # measurement column is missing so add, all rows are from measurement 1
    measurement = 1,
    StartDate = as.Date(sensor_sending_date, format = "%Y-%m-%d"),
    EndDate = as.Date(sensor_return_date, format = "%Y-%m-%d") -2
  ) %>%
  filter(StartDate <= EndDate) %>%
  select(
    AID,
    StaticSensor = static_sensor_IMEI,
    MobileSensor = dynamic_sensor_IMEI,
    StartDate,
    EndDate,
    Measurement = measurement
  )

#### Spain
es_file = file.path(metadata_dir, "Keytable_SodaqAIR_BCN.xlsx")

es_metadata <- read_xlsx(es_file) %>%
  rename(AID = ID,
         static_sensor_IMEI = `IMEI static air sensor`,
         dynamic_sensor_IMEI = `IMEI mobile air sensor`,
         sensor_sending_date = `Sensors sending date`,
         sensor_return_date = `Sensors return date`) %>%
  mutate(
    # measurement column is missing so add, all rows are from measurement 1
    Measurement = 1,
    StartDate = as.Date(sensor_sending_date, format = "%Y-%m-%d"),
    EndDate = as.Date(sensor_return_date, format = "%Y-%m-%d") -2
  ) %>%
  filter(StartDate <= EndDate) %>%
  select(
    AID,
    StaticSensor = static_sensor_IMEI,
    MobileSensor = dynamic_sensor_IMEI,
    StartDate,
    EndDate,
    Measurement
  )


#### Poland
pl_file = file.path(metadata_dir, "PM DATA PL.xlsx")

pl_metadata <- read_xlsx(pl_file) %>%
  rename(AID = ULID,
         static_sensor_IMEI = `IMEI pm1`,
         dynamic_sensor_IMEI = `IMEI pm2`,
         sensor_sending_date = `Start date 2-week measurement`,
         sensor_return_date = `End date 2-week measurement`) %>%
  mutate(
    # measurement column is missing so add, all rows are from measurement 1
    Measurement = 1,
    StartDate = as.Date(sensor_sending_date, format = "%Y-%m-%d"),
    EndDate = as.Date(sensor_return_date, format = "%Y-%m-%d") -2
  ) %>%
  filter(StartDate <= EndDate) %>%
  select(
    AID,
    StaticSensor = static_sensor_IMEI,
    MobileSensor = dynamic_sensor_IMEI,
    StartDate,
    EndDate,
    Measurement
  )


countries_metadata <- list('ES' = es_metadata,
                           'GR' = gr_metadata,
                           'CH' = ch_metadata,
                           'PL' = pl_metadata,
                           'NL' = nl_metadata)
