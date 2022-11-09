# NYC Taxi TLC Trip Record Data 
# https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# year 2011, 176mln rows, 12GB RAM

nth <- parallel::detectCores() - 2 # Ryzen 9 5950X 32 cores (=> nth = 30)
data.table::setDTthreads(nth)
y <- fst::read_fst(Rfuns::file.path(ext_path, 'us', 'nyc_taxi', '2011'))

# csv: 11.2GB (zipped: GB)
# rds default: 1.4GB
# rds uncompressed: 11.2GB
# fst: 3GB
# qs: 2.2GB

microbenchmark::microbenchmark(
    'dt1' = { data.table::setDTthreads(1); data.table::fwrite(y, './Rstorage/2011.csv') },
    'dtx' = { data.table::setDTthreads(nth); data.table::fwrite(y, './Rstorage/2011.csv') },
    'rdr' = readr::write_csv(y, './Rstorage/2011.csv'),
    'rds' = saveRDS(y, './Rstorage/2011.rds'),
    'rnc' = saveRDS(y, './Rstorage/2011.rnc', compress = FALSE),
    'fst' = fst::write_fst(y, './Rstorage/2011.fst'),
    'qs1' = qs::qsave(y, './Rstorage/2011.qs'),
    'qsx' = qs::qsave(y, './Rstorage/2011.qs', nthreads = nth),
    times = 10
)
# Unit: seconds                                                                                                                        
#  expr        min         lq       mean     median         uq        max neval    cld
#   dt1  44.307403  44.307403  47.502773  47.502773  50.698144  50.698144     2     e 
#   dtx   6.245228   6.245228   6.285629   6.285629   6.326031   6.326031     2 ab    
#   rdr  16.272764  16.272764  17.081381  17.081381  17.889997  17.889997     2   c   
#   rds 319.441888 319.441888 319.540596 319.540596 319.639304 319.639304     2      f
#   rnc  15.555151  15.555151  15.555241  15.555241  15.555332  15.555332     2   c   
#   fst   2.551925   2.551925   2.617268   2.617268   2.682611   2.682611     2 a     
#   qs1  35.947644  35.947644  36.209826  36.209826  36.472008  36.472008     2    d  
#   qsx  10.685273  10.685273  10.775099  10.775099  10.864924  10.864924     2  bc   

microbenchmark::microbenchmark(
    'dt1' = { data.table::setDTthreads(1); data.table::fread('./Rstorage/2011.csv') },
    'dtx' = { data.table::setDTthreads(nth); data.table::fread('./Rstorage/2011.csv') },
    'rdr' = readr::read_csv('./Rstorage/2011.csv'),
    'rds' = readRDS('./Rstorage/2011.rds'),
    'rnc' = readRDS('./Rstorage/2011.rnc'),
    'fst' = fst::read_fst('./Rstorage/2011.fst', as.data.table = TRUE),
    'qs1' = qs::qread('./Rstorage/2011.qs'),
    'qs6' = qs::qread('./Rstorage/2011.qs', nthreads = 6),
    'qsx' = qs::qread('./Rstorage/2011.qs', nthreads = nth),
    times = 10
)
# Unit: milliseconds
#  expr       min         lq      mean    median        uq       max neval       cld
#   dt1 15150.102 15384.9305 15805.230 15822.712 15950.497 16602.819    10       g  
#   dtx  1625.603  1804.4039  2053.498  2176.882  2234.223  2355.109    10  b       
#   rdr 23429.444 23672.0207 23752.863 23718.268 23903.303 24048.548    10        h 
#   rds 26042.728 27223.5948 27243.311 27459.466 27535.535 27862.663    10         i
#   rnc  9994.286 10206.1144 10534.082 10593.441 10886.925 11118.656    10      f   
#   fst   892.296   905.2518  1144.050  1089.277  1282.975  1609.692    10 a        
#   qs1  9140.779  9514.6906  9603.860  9570.558  9667.102 10156.028    10     e    
#   qs6  5696.388  5773.2970  5906.207  5949.036  5953.662  6108.717    10   c      
#   qsx  7913.974  8072.5697  8346.366  8401.842  8429.219  8910.961    10    d     


Rfuns::write_fst_idx('2011', cname = c('pu_month', 'pu_day'), dts = y, out_path = './Rstorage/')
microbenchmark::microbenchmark(
    'idx' = rbindlist(lapply(1:10, \(x) Rfuns::read_fst_idx('./Rstorage/2011', c(8, x)))),
    'nox' = fst::read_fst('./Rstorage/2011.fst', as.data.table = TRUE) %>% 
                      {.[pu_month == 8 & pu_day <= 10]},
    times = 10
)
# Unit: milliseconds
#  expr       min        lq      mean   median      uq       max neval cld
#   idx  149.8938  150.2021  151.1586  151.307  151.84  152.6886    10  a 
#   nox 1814.7359 1818.2034 2076.8515 2088.595 2272.15 2578.0869    10   b

lapply(c('csv', 'fst', 'qs', 'rds', 'rnc'), \(x) file.remove(paste0('./Rstorage/2011', x)))

rm(list = ls())
gc()
