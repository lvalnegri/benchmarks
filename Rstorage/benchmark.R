# NYC Taxi TLC Trip Record Data 
# https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# year 2011, 176mln rows, 12GB RAM

setwd('./Rstorage')
nth <- parallel::detectCores() - 2 # Ryzen 9 5950X 32 cores (=> nth = 30)
data.table::setDTthreads(nth)
y <- fst::read_fst(file.path(Rfuns::data_path, 'us', 'nyc_taxi', '2011'), as.data.table = TRUE)

# csv: 11.2GB
# rds default: 1.4GB
# rds uncompressed: 11.2GB
# fst: 3.0GB
# qs: 2.2GB
# parquet: 1.4GB

# WRITE benchmark
microbenchmark::microbenchmark(
    'dt1' = { data.table::setDTthreads(1); data.table::fwrite(y, '2011.csv') },
    'dtx' = { data.table::setDTthreads(nth); data.table::fwrite(y, '2011.csv') },
    'rdr' = readr::write_csv(y, '2011.csv'),
    'rds' = saveRDS(y, '2011.rds'),
    'rnc' = saveRDS(y, '2011.rnc', compress = FALSE),
    'fst' = fst::write_fst(y, '2011.fst'),
    'qs1' = qs::qsave(y, '2011.qs'),
    'qsx' = qs::qsave(y, '2011.qs', nthreads = nth),
    'prq' = arrow::write_parquet(y, '2011.prq'),
    times = 5
)
# Unit: seconds                                                                                                                        
#  expr        min         lq       mean     median         uq        max neval    cld
#   dt1  43.506698  43.601451  60.065286  59.142174  76.529121  78.470098     4     e 
#   dtx   3.885360   5.071990   5.783191   6.360932   6.494393   6.525542     4 ab    
#   rdr  20.462667  20.822410  21.702479  21.876254  22.582548  22.594739     4   cd  
#   rds 310.219050 314.627363 317.984075 319.508269 321.340787 322.700713     4      f
#   rnc  11.780769  13.585253  14.649857  15.530841  15.714461  15.756976     4 a c   
#   fst   1.730701   2.188686   2.454120   2.676605   2.719554   2.732569     4 a     
#   qs1  35.756636  36.093977  36.608101  36.665860  37.122225  37.344047     4    d  
#   qsx  10.479202  10.589914  10.835403  10.874815  11.080891  11.112778     4 a c   
#   prq  18.032731  18.181001  18.515467  18.446190  18.849933  19.136758     4  bc   

# READ benchmark
microbenchmark::microbenchmark(
    'dt1' = { data.table::setDTthreads(1); data.table::fread('2011.csv') },
    'dtx' = { data.table::setDTthreads(nth); data.table::fread('2011.csv') },
    'rdr' = readr::read_csv('2011.csv'),
    'rds' = readRDS('2011.rds'),
    'rnc' = readRDS('2011.rnc'),
    'fst' = fst::read_fst('2011.fst', as.data.table = TRUE),
    'qs1' = qs::qread('2011.qs'),
    'qs6' = qs::qread('2011.qs', nthreads = 6),
    'qsx' = qs::qread('2011.qs', nthreads = nth),
    'prq' = arrow::read_parquet('2011.prq'),
    times = 10
)
# Unit: milliseconds
#  expr        min        lq      mean    median        uq       max neval       cld
#   dt1 15420.7446 15816.832 16015.146 16055.762 16261.391 16433.290    10       g  
#   dtx  2064.1992  2194.422  2447.933  2456.580  2580.871  2851.979    10  b       
#   rdr 22780.0409 22998.714 23350.561 23376.799 23530.348 24128.407    10        h 
#   rds 26129.8743 26737.063 27347.967 27542.899 27672.828 28120.879    10         i
#   rnc 10152.7153 10461.459 10678.250 10641.959 10866.486 11235.931    10      f   
#   fst   876.3981   885.251  1141.386  1110.349  1364.307  1573.851    10 a        
#   qs1  9511.6904  9528.999  9761.151  9724.772  9892.150 10244.173    10     e    
#   qs6  5832.9755  5854.643  6285.825  6375.362  6619.106  6796.521    10   c      
#   qsx  8004.8651  8077.649  8564.712  8534.912  8946.826  9236.638    10    d     
#   prq  1994.5688  2008.023  2185.968  2052.283  2251.394  2761.076    10  b       

# QUERY benchmark (first ten days of August)
Rfuns::write_fst_idx('2011', cname = c('pu_month', 'pu_day'), dts = y)
microbenchmark::microbenchmark(
    'idx' = data.table::rbindlist(lapply(1:10, \(x) Rfuns::read_fst_idx('2011', c(8, x)))),
    'nox' = fst::read_fst('2011.fst', as.data.table = TRUE) %>% 
                      {.[pu_month == 8 & pu_day <= 10]},
    'prq' = arrow::open_dataset('2011.prq') |> 
                      dplyr::filter(pu_month == 8 & pu_day <= 10) |> 
                      dplyr::collect(),
    times = 10
)
# Unit: milliseconds
#  expr       min        lq      mean   median      uq       max neval cld
#   idx  144.7218  145.1878  156.8216  153.5852  164.5233  186.6884    10  a 
#   nox 1805.2652 1890.6643 2122.4655 2077.5755 2357.2330 2559.6328    10   b
#   prq  239.7660  242.6931  248.3015  247.7814  249.1476  261.9594    10  a 

file.remove(paste0('2011'))
lapply(c('idx', 'csv', 'fst', 'qs', 'prq', 'rds', 'rnc'), \(x) file.remove(paste0('2011.', x)))

rm(list = ls())
gc()
