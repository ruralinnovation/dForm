# dform
Programmatic interface to SEC Form D data

## What is the SEC Form D?
Regulation D is a series of rules that govern commonly used regulatory exemptions that companies can use to sell securities.  Regulation D requires that companies file a notice of their offering with the SEC using Form D. Form D submissions are publicly available after filing, including some information from the Form ID such as the ZIP code of the filing. Because Form D applications are submitted prior to the actual Form D, information from that application may be available before the contents of the form.

Companies must file a Form D using the SEC’s electronic filer system called “EDGAR” within 15 days after the first sale of securities. An amendment is required annually if the offering is ongoing for more than 12 months, or if certain of the information in the notice changes.

## Using `dform`

```

dfm <- dForm$new()

# Load available Form D filings for all quarters of 2020 and cache the results. Keep only the latest instance of accession numbers
dfm$load_data(2020, quarter = c(1:4), remove_duplicates = TRUE, use_cache = TRUE))

```
