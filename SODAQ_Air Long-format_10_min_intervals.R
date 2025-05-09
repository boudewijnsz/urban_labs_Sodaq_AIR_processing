#---------------------------------------------------------
# 1. Setup Environment
#---------------------------------------------------------
library(tidyverse)
library(jsonlite)
library(lubridate)
library(furrr)

# Login to YODA, possibly with new password at https://dgk.yoda.uu.nl/user/data_access
# Try later to replace yoda for copy on O-drive 


# Enable parallel processing
plan(multisession, workers = parallel::detectCores() - 1)

#---------------------------------------------------------
# 3. Read and Prepare Metadata
#---------------------------------------------------------
source("O:/DGK/IRAS/EEPI/Projects/Exposome-Panel Study/Datamanagement/Analysis_data_code/air_sensors_UL_centra/country_logistics_file_preprocessing.R")



country <- 'ES'
# directory with sodaq AIR sensor data export
base_folder <- paste0("C:/Users/boude004/Downloads/", country)

outdir <- paste0("C:/Users/boude004/OneDrive - Universiteit Utrecht/Documents/AIR_sensor_output", "/longformat_", country)

outfile <- paste0("Long-format_analysis-version-complete_", country, ".csv")

metadata = countries_metadata[[country]]

metadata <- metadata %>%
  filter(StartDate <= EndDate)

#---------------------------------------------------------
# 2. Helper Functions
#---------------------------------------------------------
list_files_for_date_range <- function(base_folder, imei, start_date, end_date) {
  date_sequence <- seq.Date(as.Date(start_date), as.Date(end_date), by = "day")
  year_folders <- format(date_sequence, "%Y")
  month_folders <- format(date_sequence, "%m")
  day_folders <- format(date_sequence, "%d")
  
  files <- mapply(
    function(year, month, day) {
      file.path(base_folder, year, month, day, paste0(imei, ".txt"))
    },
    year_folders,
    month_folders,
    day_folders
  )
  valid_files <- files[file.exists(files)]
  return(valid_files)
}

process_file <- function(file_path) {
  lines <- tryCatch({
    con <- file(file_path, "r")
    raw_lines <- suppressWarnings(readLines(con))
    close(con)
    raw_lines
  }, error = function(e) NULL)
  
  if (is.null(lines)) return(tibble())
  
  data <- lines %>% 
    map(jsonlite::fromJSON) %>% 
    bind_rows() %>% 
    mutate(
      created_at = as.POSIXct(created_at, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC"),
      latitude = as.numeric(lat),
      longitude = as.numeric(lon),
      pm_2_5 = as.numeric(pm_2_5)
    )
  
  return(data)
}


#---------------------------------------------------------
# 4. Process Data for Sensors
#---------------------------------------------------------
process_sensor_data <- function(sensor, start_date, end_date) {
  if (is.na(sensor)) return(tibble())  # If the sensor is NA, return an empty tibble
  
  files <- list_files_for_date_range(base_folder, sensor, start_date, end_date)
  
  if (length(files) == 0) return(tibble())  # No files found for the given range
  
  data <- tryCatch({
    future_map_dfr(files, process_file) %>%
      filter(created_at >= start_date & created_at <= end_date) %>%
      arrange(created_at)
  }, error = function(e) {
    warning(paste("Error processing files for", sensor, ":", e$message))
    return(tibble())  # Return an empty tibble in case of error
  })
  
  if (nrow(data) == 0) return(tibble())
  
  # Filter PM2.5 values <= 60 and handle outliers
  data <- data %>%
    mutate(pm_2_5 = as.numeric(pm_2_5)) %>% # Ensure numeric
    filter(pm_2_5 <= 1000 & pm_2_5 >= 1) %>%  # Filter values between 1 and 1000
    mutate(
      pm_2_5 = replace(pm_2_5, pm_2_5 > quantile(pm_2_5, 0.975, na.rm = TRUE), quantile(pm_2_5, 0.975, na.rm = TRUE)),
      pm_2_5 = replace(pm_2_5, pm_2_5 < quantile(pm_2_5, 0.025, na.rm = TRUE), quantile(pm_2_5, 0.025, na.rm = TRUE))  # Replace values below 2.5%
    )
  
  # Group data into 10-minute intervals
  data <- data %>%
    mutate(interval_start = floor_date(created_at, unit = "10 minutes")) %>%
    group_by(interval_start) %>%
    summarise(
      avg_pm2_5 = mean(pm_2_5, na.rm = TRUE),
      latitude = first(na.omit(latitude)),
      longitude = first(na.omit(longitude)),
      start_time = as.POSIXct(if (all(is.na(created_at))) NA else min(created_at, na.rm = TRUE), origin = "1970-01-01", tz = "UTC"),
      end_time = as.POSIXct(if (all(is.na(created_at))) NA else max(created_at, na.rm = TRUE), origin = "1970-01-01", tz = "UTC"),
      measurement_count = sum(!is.na(pm_2_5)),  # Count non-NA pm_2_5 values
      .groups = "drop"
    )
  
  return(data)
}

#---------------------------------------------------------
# 5. Iterate Over Metadata
#---------------------------------------------------------
results <- metadata %>%
  rowwise() %>%
  mutate(
    static_data = if (!is.na(StaticSensor)) list(process_sensor_data(StaticSensor, StartDate, EndDate)) else list(tibble()),
    mobile_data = if (!is.na(MobileSensor)) list(process_sensor_data(MobileSensor, StartDate, EndDate)) else list(tibble())
  ) %>%
  ungroup()

#---------------------------------------------------------
# 5.1 Identiy missing data 
#---------------------------------------------------------

missing_summary <- results %>%
  mutate(
    AID = as.character(AID),  # Ensure AID is a character variable
    has_static_data = !is.na(StaticSensor) & map_lgl(static_data, ~ nrow(.) > 0),
    has_mobile_data = !is.na(MobileSensor) & map_lgl(mobile_data, ~ nrow(.) > 0),
    missing_static = ifelse(!has_static_data, TRUE, FALSE),  # Indicator for missing static sensor
    missing_mobile = ifelse(!has_mobile_data, TRUE, FALSE),  # Indicator for missing mobile sensor
    missing_static_IMEI = ifelse(missing_static, StaticSensor, NA_character_),  # Add missing static IMEI
    missing_mobile_IMEI = ifelse(missing_mobile, MobileSensor, NA_character_)   # Add missing mobile IMEI
  ) %>%
  filter(missing_static | missing_mobile) %>%  # Include participants with any missing data
  select(
    AID, StartDate, EndDate, 
    missing_static, missing_mobile, 
    missing_static_IMEI, missing_mobile_IMEI
  )


missing_summary %>%
  group_by(AID, StartDate, EndDate) %>%
  summarise(count = n(), .groups = "drop") %>%
  filter(count > 1)




#---------------------------------------------------------
# 6. Rewrite the dataframe
#---------------------------------------------------------

# Unnest the static_data and mobile_data columns from the 'results' dataframe
df_static <- results %>%
  unnest(cols = static_data) %>%
  mutate(
    SensorType = "Static",
    IMEI = StaticSensor  # Add IMEI information for static sensor
  ) %>%
  rename(
    time_slot = interval_start,
    AveragePM2_5 = avg_pm2_5,
    Latitude = latitude,
    Longitude = longitude,
    StartTime = start_time,
    EndTime = end_time,
    MeasurementCount = measurement_count  # Rename the new column
  )

df_mobile <- results %>%
  unnest(cols = mobile_data) %>%
  mutate(
    SensorType = "Mobile",
    IMEI = MobileSensor  # Add IMEI information for mobile sensor
  ) %>%
  rename(
    time_slot = interval_start,
    AveragePM2_5 = avg_pm2_5,
    Latitude = latitude,
    Longitude = longitude,
    StartTime = start_time,
    EndTime = end_time,
    MeasurementCount = measurement_count  # Rename the new column
  )


# Combine the static and mobile data
df_combined <- bind_rows(df_static, df_mobile) %>%
  select(
    AID, StartDate, EndDate, SensorType, IMEI, time_slot, AveragePM2_5, MeasurementCount, Longitude, Latitude, StartTime, EndTime
  )



metadata_long <- metadata %>%
  pivot_longer(
    cols = c(StaticSensor, MobileSensor),
    names_to = "SensorTypeMetadata",  # Rename to avoid conflict
    values_to = "IMEI"
  )

df_combined <- df_combined %>%
  left_join(
    metadata_long %>% select(AID, StartDate, EndDate, IMEI, Measurement, SensorTypeMetadata),
    by = c("AID", "StartDate", "EndDate", "IMEI")
  ) %>%
  mutate(SensorType = coalesce(SensorType, SensorTypeMetadata)) %>%
  select(-SensorTypeMetadata)  # Drop the additional column


#---------------------------------------------------------
# 7. Cleaning variables
#---------------------------------------------------------

df_combined <- df_combined %>%
  mutate(
    AID = as.character(AID),
    IMEI = as.character(IMEI),
    AveragePM2_5 = round(AveragePM2_5, 2)
  )


# View the resulting dataframe
head(df_combined)


#---------------------------------------------------------
# 8. Save Results
#---------------------------------------------------------
# Save the dataframe to an Excel file
#can't be .xlsx because +1M rows 
write.csv(df_combined, file.path(outdir, paste0("Long-format_analysis-version-complete_", country, ".csv")), row.names = FALSE)

save(df_combined, file=file.path(outdir, paste0("Long-Format-10-minute-SODAQdata_", country)))
saveRDS(df_combined, file=file.path(outdir, paste0("Long-Format-10-minute-SODAQdata_RDS_", country)))
saveRDS(df_mobile, file=file.path(outdir, paste0("Long-Format-10-minute-SODAQdata-mobile_RDS_", country)))
saveRDS(df_static, file=file.path(outdir, paste0("Long-Format-10-minute-SODAQdata-static_RDS_", country)))
saveRDS(results, file=file.path(outdir, paste0("Wide-Format-Results-SODAQdata_", country)))

#---------------------------------------------------------
# 9. Add months and seasons 
#---------------------------------------------------------
# Extract month and season
df_combined$Month <- month(df_combined$time_slot, label = TRUE, abbr = FALSE)
df_combined$Season <- case_when(
  month(df_combined$time_slot) %in% c(12, 1, 2) ~ "Winter",
  month(df_combined$time_slot) %in% c(3, 4, 5)  ~ "Spring",
  month(df_combined$time_slot) %in% c(6, 7, 8)  ~ "Summer",
  month(df_combined$time_slot) %in% c(9, 10, 11) ~ "Autumn"
)

#---------------------------------------------------------
# 10. Add distance moved and speed moved
# Mobile only
#---------------------------------------------------------
library(dplyr)
library(geosphere)

df_combined <- df_combined %>%
  arrange(AID, IMEI, time_slot) %>%  
  group_by(AID, IMEI) %>%
  mutate(
    prev_Lon = lag(Longitude, order_by = time_slot),  # Get previous valid Longitude
    prev_Lat = lag(Latitude, order_by = time_slot),  # Get previous valid Latitude
    prev_time = lag(time_slot, order_by = time_slot),  # Get previous time slot
    prev_type = lag(SensorType, order_by = time_slot),  # Get previous SensorType
    TimeDiff = as.numeric(difftime(time_slot, prev_time, units = "mins")),  
    
    Distance_km = ifelse(
      SensorType == "Mobile" & prev_type == "Mobile" &  # Ensure previous row is Mobile
        TimeDiff == 10 & 
        !is.na(Longitude) & !is.na(Latitude) & 
        !is.na(prev_Lon) & !is.na(prev_Lat),
      distHaversine(cbind(Longitude, Latitude), cbind(prev_Lon, prev_Lat)) / 1000, 
      NA_real_  
    ),
    
    Speed_kmh = ifelse(!is.na(Distance_km), Distance_km * 6, NA_real_)
  ) %>%
  ungroup()

df_combined <- df_combined %>%
  mutate(
    Mode_Transport = case_when(
      Speed_kmh >= 0 & Speed_kmh <= 1 ~ "Less then 1km/h",
      Speed_kmh > 1 & Speed_kmh <= 5 ~ "Walking",
      Speed_kmh > 5 & Speed_kmh <= 25 ~ "Biking",
      Speed_kmh > 25 ~ "Motorized Vehicle",
      TRUE ~ NA_character_  # Assign NA if Speed_kmh is missing
    )
  )

#-----------------------------------------------------
# 11. Speed and months and seasons
# Mobile only
#-----------------------------------------------------
#percentage of occurance of each transport mode per month
mode_by_month <- df_combined %>%
  filter(SensorType == "Mobile", !is.na(Mode_Transport)) %>%
  group_by(Month, Mode_Transport) %>%
  summarise(Count = n(), .groups = "drop") %>%
  group_by(Month) %>%
  mutate(Proportion = Count / sum(Count) *100)

mode_by_season <- df_combined %>%
  filter(SensorType == "Mobile", !is.na(Mode_Transport)) %>%
  group_by(Season, Mode_Transport) %>%
  summarise(Count = n(), .groups = "drop") %>%
  group_by(Season) %>%
  mutate(Proportion = Count / sum(Count)*100)

#---------------------------------------------------------
# 12. Save Results + Month, season, speed & distance
#---------------------------------------------------------
# Save the dataframe to an Excel file
#can't be .xlsx because +1M rows 
write.csv(df_combined, file.path(outdir, outfile), row.names = FALSE)

save(df_combined, file=file.path(outdir, paste0("Long-Format-10-minute-SODAQdata+Extras_", country)))
saveRDS(df_combined, file=file.path(outdir, paste0("Long-Format-10-minute-SODAQdata_RDS+Extras_", country)))


