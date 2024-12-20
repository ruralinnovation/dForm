# UPDATE: User-Agent header hack is not working. Replacing download.file with CORI-brewed download_file

#' R6 Class for downloading, caching, and basic processing of SEC Form D data
#'
#' @description
#' Downloads, caches, combines, and optionally de-duplicates SEC Form D data
#'
#' @details
#' A dForm object can download Form D data, make that data available in your session, and optionally de-duplicate that data 
#' using previous accessions.
#' 
dForm <- R6::R6Class('dForm',
                     public = list(
                       #' @description Download and read Form D data for chosen years and quarters.
                       #' @param years The separator to use for value concatenation
                       #' @param quarter Quarters to download. Defaults to all quarters (1 to 4).
                       #' @param remove_duplicates If `TRUE`, the previous accession numbers in the `offerings` data will be used to de-duplicate the data
                       #' @param use_cache If `TRUE`, read data from cached downloads. Otherwise, download and load the data.
                       #' @return self for method chaining
                       #' @export
                       #'
                       load_data = function(years, quarter = c(1:4), remove_duplicates = TRUE, use_cache = TRUE){
                         
                         stopifnot(is.numeric(years))
                         stopifnot(is.numeric(quarter))
                         if (max(quarter) > 4 | min(quarter) < 1){
                           stop("Quarter must be a numeric vector or value between 1 and 4", call. = FALSE)
                         }
                         
                         private$download(years, quarter, usecache = use_cache)
                         private$load(remove_duplicates)
                         # private$make_codebook()
                         
                         return(invisible(self))
                       },
                       #' @description Aggregate all Form D data sets that are not unique on acessionnumber.
                       #' @param sep The separator to use for value concatenation
                       #' @return self for method chaining
                       #' @export
                       #'
                       aggregate_data = function(sep = ", "){
                         
                         private$aggregate('issuers', separator = sep)
                         private$aggregate('recipients', separator = sep)
                         private$aggregate('related_persons', separator = sep)
                         private$aggregate('signatures', separator = sep)
                         return(invisible(self))
                       },
                       #' @description Combine two or more component data
                       #' @param tables_to_combine The name of the data set to return a codebook for, one of 'submissions', 'issuers', 
                       #' 'offerings', 'recipients', 'related_persons', 'signatures'
                       #' @return A data
                       #' @export
                       #' @importFrom data.table setDT
                       #' 
                       combine_data = function(tables_to_combine = c('submissions', 'offerings', 'issuers')){
                         if (!all(tables_to_combine %in% private$fields)){
                           stop("`tables_to_combine` must contain only 'submissions', 'issuers', 'offerings', 'recipients', 'related_persons', 'signatures'", call. = FALSE)
                         }
                         
                         combined <- Reduce(function(x, y) merge(x, y, by = "accessionnumber"), 
                                         lapply(unique(tables_to_combine), function(x) get(x, envir = self))
                                         )
                         data.table::setDT(combined)
                         
                         return(combined)
                       },
                       #' @description Return a data frame codebook for one data set
                       #' @param data_set_name The name of the data set to return a codebook for, one of 'submissions', 'issuers', 
                       #' 'offerings', 'recipients', 'related_persons', 'signatures'
                       #' @return A data frame
                       #' @export
                       #'
                       get_codebook = function(data_set_name){
                         if (!is.character(data_set_name)){
                           stop("`data_set_name` must be one of 'submissions', 'issuers', 'offerings', 'recipients', 'related_persons', 'signatures'", call. = FALSE)
                         }
                         
                         if (!data_set_name %in% private$fields){
                           stop("`data_set_name` must be one of 'submissions', 'issuers', 'offerings', 'recipients', 'related_persons', 'signatures'", call. = FALSE)
                         }
                         
                         self$codebook[[data_set_name]]
                         
                       },
                       #' @field codebook Form D data codebook
                       codebook = NULL,
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
                       fields = c('submissions', 'issuers', 'offerings', 'recipients', 'related_persons', 'signatures'),
                       link_ptn = "https://www.sec.gov/files/structureddata/data/form-d-data-sets/{year}q{quarter}_d{suffix}.zip",
                       dir_ptn = "{year}Q{quarter}_d",
                       download = function(years, quarter, usecache){
                         
                         link_dta <- data.table::as.data.table(expand.grid(year = years, quarter = quarter))
                         link_dta[, suffix := ifelse((year < 2014) & !(year == 2012 & quarter == 1), "_0", "")]
                         
                         download_links <- glue::glue_data(link_dta, private$link_ptn)
                         dirs           <- glue::glue_data(expand.grid(year = years, quarter = quarter), private$dir_ptn)
                         
                         purrr::walk2(download_links, dirs, function(link, dir){

                           download_path  <- file.path(tempdir(), basename(link))
                           
                           # check for cached version of file
                           if (dir.exists(file.path(rappdirs::user_cache_dir(appname = 'dForm'), dir)) & usecache){
                             
                             message("!! A cached version of ", dir, " was found. Skipping download. To override this behavior, set `use_cache` = FALSE\n", sep = "")
                             
                           } else {
                             
                             # if explicitly not using cache, delete cache for re-download
                             if (dir.exists(file.path(rappdirs::user_cache_dir(appname = 'dForm'), dir)) & !usecache){
                               
                               unlink(file.path(rappdirs::user_cache_dir(appname = 'dForm'), dir), recursive = TRUE)
                               
                             } 

                             # download files
                             tryCatch({

                               message(paste0("Retrieving data from: ", link))

                               res <- download_file(link, download_path)

                               # Check res
                               if (!(download_path %in% res)) {
                                 message(paste0("Error in download result: ", res))
                               }


                             },
                             error = function(cond){
                               
                               message(crayon::red(cli::symbol$cross), " Form D data is unavailable for ", substring(basename(link), 1, 6), ". Skipping download.\n")
                               
                             })
                             
                             # unzip files
                             if (file.exists(download_path)){
                               
                               tryCatch({
                                 
                                 zip::unzip(download_path, exdir = path.expand(rappdirs::user_cache_dir(appname = 'dForm')))
                                 
                               }, error = function(cond){
                                 
                                 message(crayon::red(cli::symbol$cross), " Error extracting data for ", substring(basename(link), 1, 6), ".\n", sep = "")
                                 
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
                         # browser()
                         # get previous accession numbers for de-duplication
                         self$previous_accessions <- self$offerings[!is.na(previousaccessionnumber) & previousaccessionnumber != '', list(accessionnumber = previousaccessionnumber)]
                         
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
                       # make_codebook = function(){
                       #   dirs <- list.dirs(path.expand(rappdirs::user_cache_dir(appname = 'dForm')))
                       #   dir_to_load <- dirs[grepl("\\d{4}Q\\d_d", dirs)][[1]]
                       #
                       #   cb_path <- path.expand(file.path(dir_to_load, "FormD_readme.html"))
                       #
                       #   self$codebook <- lapply(1:6, function(tblnum){
                       #     dta <- htmltab::htmltab(cb_path, tblnum, rm_nodata_cols = FALSE)
                       #     names(dta) <- gsub("\\W+", "_", tolower(names(dta)))
                       #
                       #     return(dta)
                       #   })
                       #
                       #   names(self$codebook) <- c('submissions', 'issuers', 'offerings', 'recipients', 'related_persons', 'signatures')
                       #
                       # },
                       process_files = function(dirlist, de_dupe, de_dupe_against = NULL){
                         message(paste0("typeof dirlist: ", typeof(dirlist)))
                         basename(dirlist[1])

                         if (!is.null(dirlist) && !is.null(dirlist) && length(dirlist) > 0) {
                           fl <- gsub("\\.tsv$", "", basename(dirlist[1]))
                           message("Loading ", fl, " from cache for selected years\n", sep  = '')

                           dta <- suppressWarnings(data.table::rbindlist(lapply(dirlist, function(fp){
                             d <- data.table::fread(fp, sep = '\t', colClasses = list(character = 'FILING_DATE'))
                             fl <- unlist(strsplit(fp, "\\\\|/"))[[length(unlist(strsplit(fp, "\\\\|/"))) - 1]]

                             d[, year := as.numeric(substring(fl, 1, 4))]
                             d[, quarter := as.numeric(substring(fl, 6, 6))]

                             d
                           } )))

                           data.table::setnames(dta, names(dta), tolower(names(dta)))

                           if ('accessionnumber' %in% names(dta)){
                             data.table::setkey(dta, 'accessionnumber')
                           }

                           if (de_dupe & !is.null(de_dupe_against) & 'accessionnumber' %in% names(dta)){
                             dta <- dta[!de_dupe_against]
                           }

                           message(crayon::green(cli::symbol$tick), " ", fl, " loaded\n")
                         } else {
                           stop(crayon::red(cli::symbol$cross), " Error in directory argument for extract: ", dirlist, ".\n", sep = "")
                         }
                         
                         return(dta)
                       },
                       aggregate = function(dta, separator){
                         
                         if(is.null(self[[dta]])){
                           stop(paste0(dta, ' has not been loaded'), call. = FALSE)
                         }
                         
                         message("Aggregating ", dta, " by accessionnumber\n", sep = '')
                         
                         self[[dta]] <- self[[dta]][, lapply(.SD, paste0, collapse = separator), accessionnumber]
                         
                         message(crayon::green(cli::symbol$tick), " ", dta, "data set aggregated\n")
                         
                       }
                       
                     )
)
