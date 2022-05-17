dForm <- R6::R6Class('dForm',
                     public = list(
                       #' @description Download and read Form D data for chosen years and quarters.
                       #' @param years The separator to use for value concatenation
                       #' @return self for method chaining
                       #' @export
                       #'
                       load_data = function(years, quarter = c(1:4), remove_duplicates = TRUE, use_cache = TRUE){
                         
                         stopifnot(is.numeric(years))
                         stopifnot(is.numeric(quarter))
                         
                         private$download(years, quarter, usecache = use_cache)
                         private$load(remove_duplicates)
                         
                         return(invisible(self))
                       },
                       #' @description Aggregate all Form D data sets that are not unique on acessionnumber.
                       #' @param sep The separator to use for value concatenation
                       #' @return self for method chaining
                       #' @export
                       #'
                       aggregate_data = function(sep = ", "){
                         # cat("Aggregating issuers by accessionnumber\n")
                         # self$issuers <- self$issuers[, lapply(.SD, paste0, collapse = ", "), accessionnumber]
                         # cat(crayon::green(cli::symbol$tick), " Issuers data set aggregated\n")
                         return(invisible(self))
                       },
                       #' @field submissions Combined submission data for selected years and quarters
                       submissions = NULL,
                       #' @field issuers Combined issuers data for selected years and quarters
                       issuers = NULL,
                       #' @field offerings Combined offerings data for selected years and quarters
                       offerings = NULL, 
                       #' @field recipients Combined recipients data for selected years and quarters
                       recipients = NULL,
                       #' @field related_persons Combined related persons data for selected years and quarters
                       related_persons = NULL,
                       #' @field signatures Combined signatures data for selected years and quarters
                       signatures = NULL,
                       #' @field previous_accessions Combined previous accessions data for selected years and quarters
                       previous_accessions = NULL
                     ),
                     private = list(
                       link_ptn = "https://www.sec.gov/files/structureddata/data/form-d-data-sets/{year}q{quarter}_d.zip",
                       dir_ptn = "{year}Q{quarter}_d",
                       download = function(years, quarter, usecache){
                         
                         download_links <- glue::glue_data(expand.grid(year = years, quarter = quarter), private$link_ptn)
                         dirs           <- glue::glue_data(expand.grid(year = years, quarter = quarter), private$dir_ptn)
                         
                         purrr::walk2(download_links, dirs, function(link, dir){
                           
                           # check for cached version of file
                           if (dir.exists(file.path(rappdirs::user_cache_dir(appname = 'dForm'), dir)) & usecache){
                             
                             cat(crayon::yellow(cli::symbol$warning), " A cached version of ", dir, " was found. Skipping download. To override this behavior, set `use_cache` = FALSE\n", sep = "")
                             
                           } else {
                             
                             # if explicitly not using cache, delete cache for re-download
                             if (dir.exists(file.path(rappdirs::user_cache_dir(appname = 'dForm'), dir)) & !usecache){
                               
                               unlink(file.path(rappdirs::user_cache_dir(appname = 'dForm'), dir), recursive = TRUE)
                               
                             } 
                             
                             # download files
                             tryCatch({
                               
                               suppressWarnings(download.file(link, file.path(tempdir(), basename(link))))
                               
                             },
                             error = function(cond){
                               
                               cat(crayon::red(cli::symbol$cross), " Form D data is unavailable for ", substring(basename(link), 1, 6), ". Skipping download.\n", sep = "")
                               
                             })
                             
                             # unzip files
                             if (file.exists(file.path(tempdir(), basename(link))
                             )
                             ){
                               
                               tryCatch({
                                 
                                 zip::unzip(file.path(tempdir(), basename(link)), exdir = path.expand(rappdirs::user_cache_dir(appname = 'dForm')))
                                 
                               }, error = function(cond){
                                 
                                 cat(crayon::red(cli::symbol$cross), " Error extracting data for ", substring(basename(link), 1, 6), ".\n", sep = "")
                                 
                               })
                               
                             }
                           }
                           
                         })
                       },
                       load = function(dedupe){
                         dirs <- list.dirs(path.expand(rappdirs::user_cache_dir(appname = 'dForm')))
                         dirs_to_load <- dirs[grepl("\\d{4}Q\\d_d", dirs)]
                         # browser()
                         
                         # read offerings first because this tracks previous accession numbers needed for rough deduplication
                         self$offerings       <- private$process_files(file.path(dirs_to_load, "OFFERING.tsv"), dedupe)
                         
                         # get previous accession numbers for de-duplication
                         self$previous_accessions <- self$offerings[!is.na(previousaccessionnumber) & previousaccessionnumber != '', .(accessionnumber = previousaccessionnumber)]
                         
                         data.table::setkey(self$previous_accessions, 'accessionnumber')
                         
                         if (dedupe){
                           self$offerings <- self$offerings[!self$previous_accessions]
                         }
                         
                         self$submissions     <- private$process_files(file.path(dirs_to_load, "FORMDSUBMISSION.tsv"), dedupe, self$previous_accessions)
                         self$issuers         <- private$process_files(file.path(dirs_to_load, "ISSUERS.tsv"), dedupe, self$previous_accessions)
                         self$recipients      <- private$process_files(file.path(dirs_to_load, "RECIPIENTS.tsv"), dedupe, self$previous_accessions)
                         self$related_persons <- private$process_files(file.path(dirs_to_load, "RELATEDPERSONS.tsv"), dedupe, self$previous_accessions)
                         self$signatures      <- private$process_files(file.path(dirs_to_load, "SIGNATURES.tsv"), dedupe, self$previous_accessions)
                         
                       },
                       process_files = function(dirlist, de_dupe, de_dupe_against = NULL){
                         fl <- gsub("\\.tsv$", "",basename(dirlist[[1]]))
                         cat("Loading ", fl, " from cache for selected years\n", sep  = '')
                         
                         dta <- suppressWarnings(data.table::rbindlist(lapply(dirlist, data.table::fread, sep = '\t', colClasses = list(character = 'FILING_DATE'))))
                         
                         data.table::setnames(dta, names(dta), tolower(names(dta)))
                         
                         if ('accessionnumber' %in% names(dta)){
                           data.table::setkey(dta, 'accessionnumber')
                         }
                         
                         if (de_dupe & !is.null(de_dupe_against) & 'accessionnumber' %in% names(dta)){
                           dta <- dta[!de_dupe_against]
                         }
                         
                         cat(crayon::green(cli::symbol$tick), fl, "loaded\n", sep = ' ')
                         
                         return(dta)
                       },
                       aggregate = function(dta, separator){
                         
                         cat("Aggregating ", dta, " by accessionnumber\n", sep = '')
                         self[[dta]] <- self[[dta]][, lapply(.SD, paste0, collapse = separator), accessionnumber]
                         cat(crayon::green(cli::symbol$tick), dta, "data set aggregated\n", sep = ' ')
                         
                       }
                       
                     )
)
