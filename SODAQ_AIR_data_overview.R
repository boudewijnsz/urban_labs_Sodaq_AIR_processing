#---------------------------------------------------------
# 1. Setup Environment
#---------------------------------------------------------
library(tidyverse)
library(readxl)
library(openxlsx)
library(lubridate)
library(dplyr)

# this is a modified version of the script "20241213 - eindscript + cleaning.R"
# created by Nina van Wilgen

# Login to YODA, possibly with new password at https://dgk.yoda.uu.nl/user/data_access
# getwd()
# setwd()
# Log function to capture execution details - saves at working directory
log_message <- function(message, log_file = "debug_log9.txt") {
  write(paste(Sys.time(), "-", message), file = log_file, append = TRUE)
}


# Define file paths
# csv_path is the csv file with the following columns: 
# AID, static_sensor_IMEI, dynamic_sensor_IMEI, sensor_sending_date, sensor_return_date, measurement
# measurement is the measurement round for each participant 
# sensor_sending_date and sensor_return_date should be dates in excel 
# base_folder is the raw .txt file data which should be in the same folder structure year, month, day, IMEI

# proprocessing metadata files is performed in different script, run first
source("O:/DGK/IRAS/EEPI/Projects/Exposome-Panel Study/Datamanagement/Analysis_data_code/air_sensors_UL_centra/country_logistics_file_preprocessing.R")

outdir <- "O:/DGK/IRAS/EEPI/Privacy/Exposome-Panel Study/Datamanagement/data/raw/air_sensors_UL_centra/results"

country <- 'NL'
filename <- paste0("SODAQAIR_SUMMARY_", country, ".csv")
# directory with sodaq AIR sensor data export
base_folder <- paste0("C:/Users/boude004/Downloads/", country)

metadata = countries_metadata[[country]]

metadata <- metadata %>%
  filter(StartDate <= EndDate)

# Verify the filtered data
print(metadata)

#---------------------------------------------------------
# 3. Define Helper Functions
#---------------------------------------------------------
list_files_for_date_range <- function(base_folder, imei, start_date, end_date) {
  # Generate sequence of dates between start and end
  date_sequence <- seq.Date(as.Date(start_date), as.Date(end_date), by = "day")
  
  # Extract year, month, and day for each date
  year_folders <- format(date_sequence, "%Y")
  month_folders <- format(date_sequence, "%m")
  day_folders <- format(date_sequence, "%d")
  
  # Construct file paths with year included
  files <- mapply(
    function(year, month, day) {
      file.path(base_folder, year, month, day, paste0(imei, ".txt"))
    },
    year_folders,
    month_folders,
    day_folders
  )
  
  # Filter only existing files
  valid_files <- files[file.exists(files)]
  
  return(valid_files)
}


# This function processes both static and mobile sensor data
process_sensor_data <- function(file_paths, start_date, end_date) {
  if (length(file_paths) == 0) {
    return(tibble(
      avg_pm2_5 = NA,
      hours_measured = 0,
      first_date = NA,
      last_date = NA,
      total_hours_possible = as.numeric(difftime(end_date, start_date, units = "hours")),
      percentage_measured = 0,
      latitude = NA,
      longitude = NA
    ))
  }
  # Log the files being processed
  log_message(paste("Processing the following files:", paste(file_paths, collapse = ", ")))
  
  # Helper function to find the first valid latitude or longitude
  get_valid_location <- function(lat_long_values) {
    valid_values <- lat_long_values[!is.na(lat_long_values) & lat_long_values != 0]
    if (length(valid_values) > 0) {
      return(valid_values[1])  # Return the first valid value
    } else {
      return(NA)  # Return NA if no valid values are found
    }
  }
  
  data <- file_paths %>% 
    map_dfr(~ {
      lines <- tryCatch({
        con <- file(.x, "r")
        raw_lines <- suppressWarnings(readLines(con))
        close(con)
        raw_lines
      }, error = function(e) NULL)
      
      if (is.null(lines)) return(tibble())
      lines %>% 
        map(jsonlite::fromJSON) %>% 
        bind_rows() %>% 
        mutate(
          created_at = as.POSIXct(created_at, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC"),
          latitude = as.numeric(lat),   # Convert lat to numeric
          longitude = as.numeric(lon)   # Convert lon to numeric
        )
    })
  
  if (nrow(data) == 0) {
    return(tibble(
      avg_pm2_5 = NA,
      hours_measured = 0,
      first_date = NA,
      last_date = NA,
      total_hours_possible = as.numeric(difftime(end_date, start_date, units = "hours")),
      percentage_measured = 0,
      latitude = NA,
      longitude = NA
    ))
  }
  
  # Ensure pm_2_5 is numeric
  data <- data %>%
    mutate(pm_2_5 = as.numeric(pm_2_5)) %>%
    mutate(Date = as.Date(created_at)) %>%
    group_by(Date) %>%
    mutate(DailyMean = mean(pm_2_5, na.rm = TRUE)) %>%
    ungroup() %>%
    filter(DailyMean <= 60) %>%
    mutate(
      pm_2_5 = replace(pm_2_5, pm_2_5 > quantile(pm_2_5, 0.999, na.rm = TRUE), 
                       quantile(pm_2_5, 0.999, na.rm = TRUE)),
      pm_2_5 = replace(pm_2_5, pm_2_5 < 0.1, NA)
    )
  
  # Filter data for the specified time range
  filtered_data <- data %>%
    filter(created_at >= start_date & created_at <= end_date)
  
  if (nrow(filtered_data) == 0) {
    return(tibble(
      avg_pm2_5 = NA,
      hours_measured = 0,
      first_date = NA,
      last_date = NA,
      total_hours_possible = as.numeric(difftime(end_date, start_date, units = "hours")),
      percentage_measured = 0,
      latitude = NA,
      longitude = NA
    ))
  }
  
  # Calculate total hours for static data
  total_hours_static <- filtered_data %>%
    arrange(created_at) %>%
    mutate(
      timediff = created_at - lag(created_at),
      diff_secs = as.numeric(timediff, units = "secs")
    ) %>%
    filter(!is.na(diff_secs) & diff_secs <= 600) %>%
    summarise(TotalHoursStatic = sum(diff_secs, na.rm = TRUE) / 3600) %>%
    pull(TotalHoursStatic)
  
  # Extract first valid latitude and longitude
  valid_latitude <- get_valid_location(filtered_data$latitude)
  valid_longitude <- get_valid_location(filtered_data$longitude)
  
  summary <- filtered_data %>%
    summarise(
      avg_pm2_5 = mean(pm_2_5, na.rm = TRUE),
      hours_measured = n_distinct(floor_date(created_at, "hour")),
      first_date = min(created_at),
      last_date = max(created_at)
    )
  
  summary$total_hours_possible <- as.numeric(difftime(end_date, start_date, units = "hours"))
  summary$percentage_measured <- (summary$hours_measured / summary$total_hours_possible) * 100
  
  # Add first valid location
  summary$latitude <- valid_latitude
  summary$longitude <- valid_longitude
  
  # Round the relevant columns to 2 decimal places
  summary$avg_pm2_5 <- round(summary$avg_pm2_5, 2)
  summary$percentage_measured <- round(summary$percentage_measured, 2)
  
  return(summary)
}


#---------------------------------------------------------
# 4. Iterate over rows for Measurement Round 1 
# + Skip if static and mobile is NA keep if one is NA 
# + remove invalid enddate
#---------------------------------------------------------
Sys.setenv("VROOM_CONNECTION_SIZE" = 131072 * 2)

results <- metadata %>%
  # Remove invalid date ranges (where StartDate > EndDate)
  filter(StartDate <= EndDate) %>%
  # Filter for rows where measurement == 1
  filter(Measurement == 1) %>%
  # Filter out rows where both StaticSensor and MobileSensor are NA
  filter(!(is.na(StaticSensor) & is.na(MobileSensor))) %>%
  rowwise() %>%
  mutate(
    # Get file paths for static sensor if it exists
    static_files = if (!is.na(StaticSensor)) list(list_files_for_date_range(base_folder, StaticSensor, StartDate, EndDate)) else list(NULL),
    
    # Get file paths for mobile sensor if it exists
    mobile_files = if (!is.na(MobileSensor)) list(list_files_for_date_range(base_folder, MobileSensor, StartDate, EndDate)) else list(NULL),
    
    # Process static sensor data
    static_summary = if (!is.na(StaticSensor)) list(process_sensor_data(static_files, StartDate, EndDate)) else list(tibble(
      avg_pm2_5 = NA,
      hours_measured = NA,
      first_date = NA,
      last_date = NA,
      total_hours_possible = as.numeric(difftime(EndDate, StartDate, units = "hours")),
      percentage_measured = NA,
      latitude = NA,
      longitude = NA
    )),
    
    # Process mobile sensor data
    mobile_summary = if (!is.na(MobileSensor)) list(process_sensor_data(mobile_files, StartDate, EndDate)) else list(tibble(
      avg_pm2_5 = NA,
      hours_measured = NA,
      first_date = NA,
      last_date = NA,
      total_hours_possible = as.numeric(difftime(EndDate, StartDate, units = "hours")),
      percentage_measured = NA,
      latitude = NA,
      longitude = NA
    ))
  ) %>%
  # Unnest the lists to expand the summaries into separate columns
  unnest(cols = c(static_summary, mobile_summary), names_sep = "_") %>%
  # Rename the columns for clarity
  rename(
    static_avg_pm2_5 = static_summary_avg_pm2_5,
    static_hours_measured = static_summary_hours_measured,
    static_percentage_measured = static_summary_percentage_measured,
    static_first_date = static_summary_first_date,
    static_last_date = static_summary_last_date,
    
    mobile_avg_pm2_5 = mobile_summary_avg_pm2_5,
    mobile_hours_measured = mobile_summary_hours_measured,
    mobile_percentage_measured = mobile_summary_percentage_measured,
    mobile_first_date = mobile_summary_first_date,
    mobile_last_date = mobile_summary_last_date,
    
    total_hours_possible = static_summary_total_hours_possible,
    static_latitude = static_summary_latitude,
    static_longitude = static_summary_longitude
  ) %>%
  # Select the relevant columns for the results
  select(
    AID,
    StartDate,
    EndDate,
    total_hours_possible,
    StaticSensor,
    static_avg_pm2_5,
    static_hours_measured,
    static_percentage_measured,
    static_first_date,
    static_last_date,
    static_latitude,
    static_longitude,
    MobileSensor,
    mobile_avg_pm2_5,
    mobile_hours_measured,
    mobile_percentage_measured,
    mobile_first_date,
    mobile_last_date
  )

log_message("Results calculated and processed.")

#---------------------------------------------------------
# 5. Clean data  
#---------------------------------------------------------
#Make sure static sensor, mobile sensor and AID is a character not a numeric variable
results <- results %>%
  mutate(
    StaticSensor = as.character(StaticSensor),
    MobileSensor = as.character(MobileSensor),
    AID = as.character(AID)
  )

#if 0 hours measured and 0 percent of hours measured it's NA
results <- results %>%
  mutate(
    static_hours_measured = na_if(static_hours_measured, 0),
    mobile_hours_measured = na_if(mobile_hours_measured, 0),
    static_percentage_measured = na_if(static_percentage_measured,0),
    mobile_percentage_measured = na_if(mobile_percentage_measured,0)
  )


# View and export results
view(results)

#---------------------------------------------------------
# 6. Save summary 
#---------------------------------------------------------
write.csv2(results, 
           file = file.path(outdir, filename))

log_message("Summary exported successfully.")
log_message("Script execution completed.")
