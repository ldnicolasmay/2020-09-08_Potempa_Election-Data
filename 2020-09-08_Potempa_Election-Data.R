# MICHIGAN BOARD OF ELECTIONS DATA 2020 ----

# USEFUL LIBRARIES ----
library(data.table)
setDTthreads(3)
library(magrittr)


brd_elex_data_path <- 
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

# _ LOAD DATA ----

cols_cnty_20 <-
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
cols_cnty_20_ <- cols_cnty_20
cols_cnty_20_[cols_cnty_20_ == "VOTER_ID"] <- "VOTER_ID\""

counties <- c(
  "Livingston",
  "Macomb",
  "Monroe",
  "Oakland",
  "St Clair",
  "Washtenaw",
  "Wayne"
)

fxn_fread <- function(path, select_cols, name_cols) {
  fread(path, select = select_cols, col.names = name_cols, colClasses = "character")
}

df_county_2020_data <- counties %>>% 
  (paste0(brd_elex_data_path, "/raw_data_2020/", ., " County.csv")) %>>% 
  purrr::map_dfr(fxn_fread, select_cols = cols_cnty_20, name_cols = cols_cnty_20)

counties_ <- c(
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

df_county_2020_data_ <- counties_ %>>%
  (paste0(brd_elex_data_path, "/raw_data_2020/", ., " QVF 8.2020.csv")) %>>%
  purrr::map_dfr(fxn_fread, select_cols = cols_cnty_20_, name_cols = cols_cnty_20)

df_county_2020 <- rbindlist(list(df_county_2020_data, df_county_2020_data_))
rm(df_county_2020_data); rm(df_county_2020_data_)


