library(remotes)
library(tigris)
library(sf)
library(dform)
library(janitor)
library(dplyr)
library(coriverse)
library(stringr)
library(readxl)
library(readr)
library(googlesheets4)
library(Rto)
library(tidyr)
library(scales)

# Loading Offerings and Issuers Form D data ------------------------------------
vc <- dForm$new()
vc$load_data(2022:2019)
issuers <- vc$issuers %>%
  filter(is_primaryissuer_flag != 'NO')
offerings <- vc$offerings
off_iss <- offerings %>%
  left_join(issuers, by = 'accessionnumber')

# Loading County Demographic Data ----------------------------------------------
con <- connect_to_db("acs")
acs_data <- read_acs(con, c('total_population',
                            'diversity_score',
                            'race_white_non_hispanic_pct',
                            'total_white_non_hispanic',
                            'non_white_and_non_hispanic_pct',
                            'non_white_and_non_hispanic',
                            'hispanic_pct',
                            'total_hispanic',
                            'race_black_or_african_american_non_hispanic_pct',
                            'total_af_am_non_hispanic',
                            'race_american_indian_and_alaska_native_non_hispanic_pct',
                            'total_am_ind_non_hispanic',
                            'race_asian_non_hispanic_pct',
                            'total_asian_non_hispanic',
                            'race_native_hawaiian_and_other_pacific_islander_non_hispanic_pct',
                            'total_hawaiian_other_pi_non_hispanic',
                            'race_some_other_race_non_hispanic_pct',
                            'total_other_non_hispanic',
                            'two_or_more_races_non_hispanic_pct',
                            'total_two_or_more_races_non_hispanic',
                            'total_exactly_two_races_incl_other_non_hispanic',
                            'total_two_or_more_races_excl_other_non_hispanic'),
                     tidy = TRUE,
                     year = 2019,
                     geography = "county") %>% 
  select(!year) %>% 
  mutate(across(diversity_score:two_or_more_races_non_hispanic_pct, round, 4))
acs_data <- acs_data %>% 
  mutate(race_white_non_hispanic_pct = total_white_non_hispanic/total_population)

# Cleaning zip codes and locations in Form D data ------------------------------
states <- c(state.abb, 'DC')
off_iss_clean <- off_iss
off_iss_clean$stateorcountry[off_iss_clean$stateorcountrydescription == 'GEORGIA'] <- 'GA' # several Georgia states were written as '2Q' instead of 'GA'
off_iss_clean <- off_iss_clean %>%
  filter(stateorcountry %in% states) # Removing all the international and US territory observations
off_iss_clean$zipcode <- str_replace(off_iss_clean$zipcode, '-\\d+$', '') # Turning xxxxx-xxxx zip codes into xxxxx zip codes
off_iss_clean <- off_iss_clean %>%
  mutate(zipcode = str_replace_all(string = zipcode, pattern = '[a-zA-Z]', replacement = "")) %>%
  filter(zipcode != '',
         str_detect(zipcode, '\\d{5}'),
         nchar(zipcode) == 5) # Removing zip codes that are empty, w/ letters, and less than 5 digits

# For crosswalks - Postal Zip Code to ZCTA, ZCTA to County/CBSA ----------------
## Postal Zip Code to ZCTA -----------------------------------------------------
#Postal zip code to zcta crosswalk so I can match counties to the postal zip codes in Form D
zip_to_zcta <- read_xlsx('ZiptoZcta_Crosswalk_2021.xlsx') %>% 
  clean_names() %>% 
  filter(state %in% states) %>%
  rename(geoid_zcta = zcta,
         zipcode = zip_code)
## ZCTA to County --------------------------------------------------------------
zcta_co_url = "https://www2.census.gov/geo/docs/maps-data/data/rel/zcta_county_rel_10.txt"
zcta_co_relationship_file <-  read_csv(zcta_co_url) %>% 
  select(geoid_zcta = ZCTA5,
         geoid_co = GEOID,
         pct_zcta_pop_per_county = ZPOPPCT)
unique_zcta_to_county <- zcta_co_relationship_file %>% 
  group_by(geoid_zcta) %>% 
  slice_max(pct_zcta_pop_per_county) %>% 
  select(geoid_zcta, geoid_co)
## Postal Zip to ZCTA to County ------------------------------------------------
zip_to_county <- zip_to_zcta %>%
  left_join(unique_zcta_to_county, by = 'geoid_zcta') %>% 
  select(zipcode, geoid_zcta, geoid_co)
## CBSA to County --------------------------------------------------------------
cbsas <- tigris::core_based_statistical_areas(year = 2020) %>%
  st_drop_geometry() %>%
  clean_names() %>%
  select(geoid_cbsa = cbsafp,
         rurality_designation = lsad)
counties <- tigris::counties(year = 2020) %>%
  st_drop_geometry() %>%
  clean_names() %>% 
  select(geoid_co = geoid,
         county = namelsad,
         geoid_cbsa = cbsafp,
         lat_co = intptlat,
         lon_co = intptlon)
counties_cbsas <- counties %>% 
  left_join(cbsas, by = 'geoid_cbsa') %>%
  mutate(rurality_designation = case_when(
    is.na(rurality_designation) ~ "Non-CBSA", 
    rurality_designation == 'M1' ~ 'Metro',
    rurality_designation == 'M2' ~ 'Micro'
  )) 
## Postal Zip Code to ZCTA to County to CBSA -----------------------------------
zctas <- tigris::zctas(year = 2020) %>% 
  select(geoid_zcta = GEOID20,
         lat_zcta = INTPTLAT20,
         lon_zcta = INTPTLON20)
geographies <- zip_to_county %>%
  left_join(counties_cbsas, by = 'geoid_co') %>% 
  left_join(zctas, by = 'geoid_zcta')

# Joining location info and Financial data -------------------------------------
fin_data_and_locations <- off_iss_clean %>%
  inner_join(geographies, by = 'zipcode')

# Venture Capital: Date, Business, and Location --------------------------------
date_biz_locations <- fin_data_and_locations %>%
  select(sale_date,
         accession_number = accessionnumber,
         entity_name = entityname,
         industry_group_type = industrygrouptype,
         zipcode,
         geoid_zcta,
         geoid_co,
         county,
         rurality_designation,
         lat_zcta,
         lon_zcta,
         lat_co,
         lon_co,
         totalofferingamount,
         totalamountsold,
         totalremaining)

# Writing CSV for Form D locations ---------------------------------------------
write_csv(date_biz_locations, 'FormD Locations and Funding Amounts.csv')

# Creating summary stats -------------------------------------------------------
formd_tech_data <- date_biz_locations %>% 
  filter(industry_group_type == 'Other Technology')
formd_tech_counties <- formd_tech_data %>% 
  group_by(geoid_co) %>% 
  summarise(company_count = length(unique(entity_name)),
            total_amt_sold = sum(totalamountsold),
            avg_amt_sold = mean(totalamountsold)) 

# Adding County demographic info------------------------------------------------
## Formatting percentage points
acs_data$hispanic_pctpoint <- percent(acs_data$hispanic_pct, accuracy = .01)
acs_data$non_white_nh_pctpoint <- percent(acs_data$non_white_and_non_hispanic_pct, accuracy = .01)
acs_data$american_indian_alaskan_native_nh_pctpoint <- percent(acs_data$race_american_indian_and_alaska_native_non_hispanic_pct, accuracy = .01)
acs_data$asian_nh_pctpoint <- percent(acs_data$race_asian_non_hispanic_pct, accuracy = .01)
acs_data$black_africanam_nh_pctpoint <- percent(acs_data$race_black_or_african_american_non_hispanic_pct, accuracy = .01)
acs_data$native_hawaiian_pacific_islander_nh_pctpoint <- percent(acs_data$race_native_hawaiian_and_other_pacific_islander_non_hispanic_pct, accuracy = .01)
acs_data$other_race_nh_pctpoint <- percent(acs_data$race_some_other_race_non_hispanic_pct, accuracy = .01)
acs_data$two_or_more_races_nh_pctpoint <- percent(acs_data$two_or_more_races_non_hispanic_pct, accuracy = .01)
acs_data$white_nh_pctpoint <- percent(acs_data$race_white_non_hispanic_pct, accuracy = .01)
acs_data$diversity_score_pctpoint <- percent(acs_data$diversity_score, accuracy = .01)
## CBSA to County --------------------------------------------------------------
cbsas <- tigris::core_based_statistical_areas(year = 2020) %>%
  st_drop_geometry() %>%
  clean_names() %>%
  select(geoid_cbsa = cbsafp,
         rurality_desig = lsad,
         cbsa_name = name)
counties <- tigris::counties(year = 2020) %>%
  st_drop_geometry() %>%
  clean_names() %>% 
  select(geoid_co = geoid,
         statefp,
         county = namelsad,
         geoid_cbsa = cbsafp,
         lat_co = intptlat,
         lon_co = intptlon)
counties_cbsas <- counties %>% 
  left_join(cbsas, by = 'geoid_cbsa') %>%
  mutate(rurality_desig = case_when(
    is.na(rurality_desig) ~ "Non-CBSA", 
    rurality_desig == 'M1' ~ 'Metro',
    rurality_desig == 'M2' ~ 'Micro'))
counties_cbsas$cbsa_name[counties_cbsas$rurality_desig == 'Non-CBSA'] <- 'NA'
states <- tigris::states(year = 2020) %>%
  st_drop_geometry() %>%
  clean_names() %>% 
  select(statefp, stusps)
counties_states_cbsas <- counties_cbsas %>% 
  left_join(states, by = 'statefp')
counties_states_cbsas <- counties_states_cbsas %>% 
  mutate(county_state = paste0(county, ', ', stusps))
acs_counties_full <- counties_states_cbsas %>% 
  left_join(acs_data, by = 'geoid_co')

# Joining Form D data and ACS --------------------------------------------------
formd_and_acs <- acs_counties_full %>% 
  left_join(formd_tech_counties, by = 'geoid_co') 
formd_and_acs$company_count[is.na(formd_and_acs$company_count)] <- 0
formd_and_acs$total_amt_sold[is.na(formd_and_acs$total_amt_sold)] <- 0
formd_and_acs$avg_amt_sold[is.na(formd_and_acs$avg_amt_sold)] <- 0
formd_and_acs$cbsa_name[formd_and_acs$rurality_desig == 'Non-CBSA'] <- 'NA'

# Cartographic Boundaries for Counties -----------------------------------------
cartographic_boundaries <- tigris::counties(cb = TRUE, year = '2020') %>% 
  select(geoid_co = GEOID)
formd_and_acs_cb <- cartographic_boundaries %>% 
  right_join(formd_and_acs, by = 'geoid_co')

# Writing to Carto -------------------------------------------------------------
ptr <- carto$new()
ptr$write_carto(formd_and_acs_cb, 'formd_acs_and_all_counties')



