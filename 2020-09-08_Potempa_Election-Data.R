# MICHIGAN BOARD OF ELECTIONS DATA 2020 ----

# USEFUL LIBRARIES ----
library(data.table); setDTthreads(3)
library(pipeR)
library(purrr)
library(stringr)
library(lubridate)
library(readr)


# EXTRACT ----

# _ Define source data path ----

data_dir <- 
  c(
    "",
    "Users",
    "ldmay",
    "Box",
    "Documents",
    "I-CONECT",
    "Recruitment",
    "Board of Elections data - Michigan"
  ) %>>% paste0(collapse = "/")

# _ Define data columns ----

cols_county <-
  c(
    "LASTNAME",
    "FIRSTNAME",
    "MIDDLENAME",
    "NAME_SUFFIX",
    "YOB",
    "GENDER",
    "REGISTRATION_DATE",
    "STREET_NUMBER_PREFIX",
    "STREET_NUMBER",
    "STREET_NUMBER_SUFFIX",
    "DIRECTION_PREFIX",
    "STREET_NAME",
    "STREET_TYPE",
    "DIRECTION_SUFFIX",
    "EXTENSION",
    "CITY",
    "STATE",
    "ZIP_CODE",
    "MAILING_ADDRESS_LINE_ONE",
    "MAILING_ADDRESS_LINE_TWO",
    "MAILING_ADDRESS_LINE_THREE",
    "MAILING_ADDRESS_LINE_FOUR",
    "MAILING_ADDRESS_LINE_FIVE",
    "VOTER_ID",
    "COUNTY_CODE",
    "JURISDICTION_CODE",
    "WARD",
    "SCHOOL_DISTRICT_CODE",
    "STATE_HOUSE_DISTRICT_CODE",
    "STATE_SENATE_DISTRICT_CODE",
    "US_CONGRESS_DISTRICT_CODE",
    "COUNTY_COMMISIONER_DISTRICT_NAME",
    "VILLAGE_DISTRICT_CODE",
    "VILLAGE_PRECINCT",
    "SCHOOL_PRECINCT",
    "IS_PERMANENT_ABSENTEE_VOTER",
    "VOTER_STATUS_TYPE_CODE",
    "UOVACAVA_STATUS_CODE"
  )
cols_county_ <- cols_county
cols_county_[cols_county_ == "VOTER_ID"] <- "VOTER_ID\""

# _ Define county names to iterate over ----

counties_a <- c(
  "Livingston",
  "Macomb",
  "Monroe",
  "Oakland",
  "St Clair",
  "Washtenaw",
  "Wayne"
)

counties_b <- c(
  "Alpena",
  "Bay",
  "Berrien",
  "Cass",
  "Genesee",
  "Grand Traverse",
  "Kalamazoo",
  "Kent",
  "Muskegon",
  "Ottawa",
  "Roscommon",
  "Saginaw"
)

# _ Read data ----

df_county_2020_a <- counties_a %>>% 
  (paste0(data_dir, "/raw_data_2020/", ., " County.csv")) %>>% 
  map_dfr(fread, select = cols_county, col.names = cols_county, colClasses = "character")

df_county_2020_b <- counties_b %>>%
  (paste0(data_dir, "/raw_data_2020/", ., " QVF 8.2020.csv")) %>>%
  map_dfr(fread, select = cols_county_, col.names = cols_county, colClasses = "character")

df_county_2020 <- rbindlist(list(df_county_2020_a, df_county_2020_b))

# _ Clean up ----

rm(df_county_2020_a); rm(df_county_2020_b)

# _ Verify basic data facts ----
# print(object.size(df_county_2020), units = "auto")
# assertthat::are_equal(indices(df_county_2020), NULL)


# TRANSFORM ----

# _ Mutate ----

# _ _ YOB as integer ----
df_county_2020[, YOB := as.integer(YOB)]

# _ _ REGISTRATION_DATE as date
regex_date <- "\\d{1,2}/\\d{1,2}/\\d{2,4}"
df_county_2020[, REGISTRATION_DATE := str_extract(REGISTRATION_DATE, regex_date)]
df_county_2020[, REGISTRATION_DATE := mdy(regis_date_str)]

# _ _ COUNTY_CODE as integer ----
df_county_2020[, COUNTY_CODE := as.integer(COUNTY_CODE)]

# _ _ ZIP_CODE as integer ----
# (OK because no Michigan ZIPs start with 0s)
df_county_2020[, ZIP_CODE := as.integer(ZIP_CODE)]

# _ Filter ----

# _ _ Filter for >= 50 years old and <= 100 years old ----
this_year <- 2020L
df_county_2020_flt <- df_county_2020[YOB <= this_year - 50L & YOB >= this_year - 100L, ]

# _ Select fields ----

df_county_2020_slc <- 
  df_county_2020_flt[, .(VOTER_ID,
                         FIRSTNAME, MIDDLENAME, LASTNAME, NAME_SUFFIX,
                         STREET_NUMBER_PREFIX, STREET_NUMBER, STREET_NUMBER_SUFFIX,
                         DIRECTION_PREFIX, STREET_NAME, STREET_TYPE, DIRECTION_SUFFIX,
                         EXTENSION, CITY, STATE, ZIP_CODE)]
# print(object.size(df_county_2020_slc), units = "auto")


# LOAD ----

# _ Get non-repeated random samples ----

set.seed(1)
df_county_2020_slc_resample <- df_county_2020_slc %>>% 
  dplyr::sample_n(size = nrow(.), replace = FALSE)

# _ Group resampled data into n=3000 and write to CSVs ----

n <- 3000L
target_dir <- paste0("./samples_2020_n", as.character(n))

csv_nums <- 0:(floor(nrow(df_county_2020_slc_resample)/n))
nchar_max <- nchar(max(csv_nums))

# _ Get 
walk(csv_nums,
     function(num) {
       csv_path <- paste0(target_dir, "/recruits_", str_pad(num, nchar_max, pad = "0"), ".csv")
       first_row <- num * n + 1
       last_row <- if (num * n <= nrow(df_county_2020_slc_resample)) { 
         num * n + n 
       } else {
         nrow(df_county_2020_slc_resample)
       }
       
       write_csv(df_county_2020_slc_resample[first_row:last_row, ], path = csv_path,
                 na = "", append = FALSE, col_names = TRUE)
     })
