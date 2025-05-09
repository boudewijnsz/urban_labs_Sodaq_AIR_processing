#---------------------------------------------------------
# 1. Setup Environment
#---------------------------------------------------------
library(tidyverse)
library(jsonlite)
library(furrr)

# this is a modified version of the script "20250310 - Baseform SODAQ Air dataframe.r"
# created by Nina van Wilgen

# Define file paths
# proprocessing metadata files is performed in different script, run first
source("O:/DGK/IRAS/EEPI/Projects/Exposome-Panel Study/Datamanagement/Analysis_data_code/air_sensors_UL_centra/country_logistics_file_preprocessing.R")

outdir <- "O:/DGK/IRAS/EEPI/Privacy/Exposome-Panel Study/Datamanagement/data/raw/air_sensors_UL_centra/results"

country <- 'NL'
filename <- paste0("SODAQAIR_SUMMARY_", country, ".csv")
# directory with sodaq AIR sensor data export
base_folder <- paste0("C:/Users/boude004/Downloads/", country)

outfile <- paste0("Baseform_SODAQ_AIR_dataframe_", country)

metadata = countries_metadata[[country]]

metadata <- metadata %>%
  filter(StartDate <= EndDate)


# Enable parallel processing
plan(multisession, workers = parallel::detectCores() - 1)

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
      pm_2_5 = as.numeric(pm_2_5),
      humidity = as.numeric(humidity),
      temperature = as.numeric(temperature)
    )
  
  return(data)
}


#---------------------------------------------------------
# 3. Read and Prepare Metadata
#---------------------------------------------------------

#---------------------------------------------------------
# 4. Process Data for Sensors
#---------------------------------------------------------
process_sensor_data <- function(sensor, start_date, end_date) {
  if (is.na(sensor)) return(tibble())
  
  files <- list_files_for_date_range(base_folder, sensor, start_date, end_date)
  
  if (length(files) == 0) return(tibble())
  
  data <- tryCatch({
    future_map_dfr(files, process_file) %>%
      filter(created_at >= start_date & created_at <= end_date) %>%
      arrange(created_at)
  }, error = function(e) {
    warning(paste("Error processing files for", sensor, ":", e$message))
    return(tibble())
  })
  
  if (nrow(data) == 0) return(tibble())
  
  # Ensure PM2.5 is numeric and filter values
  data <- data %>%
    mutate(pm_2_5 = as.numeric(pm_2_5)) %>%
    filter(pm_2_5 <= 500 & pm_2_5 >= 1) %>%
    select(created_at, pm_2_5, humidity, temperature, latitude, longitude) %>%
    arrange(created_at)
  
  return(data)  # Returning raw data without grouping or averaging
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

saveRDS(results, file = file.path(outdir, outfile))