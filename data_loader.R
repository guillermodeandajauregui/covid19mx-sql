library(tidyverse)
library(vroom)
library(lubridate)
library(DBI)
library(odbc)
library(RMariaDB)

###############################################################################
#connect to the database ---- 
###############################################################################


con <- dbConnect(RMariaDB::MariaDB(), 
                 user = [USER], 
                 password = [PASSWORD],
                 dbname = [DB_NAME]
                 group  = [GROUP]
)

###############################################################################
#download data
###############################################################################

el_path <- "http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/datos_abiertos_covid19.zip"

temp <- tempfile()
old_timeout <- getOption("timeout")
options(timeout = Inf)
download.file(url = el_path, destfile = temp, timeout = Inf)
options(timeout = old_timeout)

#descomprime el archivo ----
el_file <- unzip(zipfile = temp, list = F, exdir = "/tmp")

header_tabla <- vroom::vroom(file = el_file, n_max = 1)

#crea la tablita y echale el overwrite ----
dbWriteTable(conn = con, 
             name = "sisver_public", 
             value = header_tabla,   
             overwrite = T
             )

dbSendStatement(conn = con, statement = "DELETE FROM sisver_public")

#hacer la query ----
mi_query <- 
  glue::glue("
           LOAD DATA LOCAL INFILE \"{el_file}\"
           INTO TABLE sisver_public
           FIELDS TERMINATED BY ',' 
           ENCLOSED BY '\"'
           LINES TERMINATED BY '\\n';
           ")

dbSendStatement(conn = con, statement = mi_query)

unlink(temp)
unlink(el_file)
rm(temp, el_file)
