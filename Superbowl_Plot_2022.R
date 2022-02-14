
# Bring in libraries
library(data.table)
library(dygraphs)
suppressMessages(library(utils))
suppressMessages(suppressPackageStartupMessages(library(arrow,quietly=TRUE)))

# Extract current files
all_files <- fread(cmd="aws s3 ls s3://havas-data-science/Super_Bowl/SB_22/Parquet/ --recursive", header = FALSE)
all_files <- all_files[V4 %like% ".parquet"]
all_files[, Time := substr(V4, nchar(V4)-22, nchar(V4)-8)]
all_files[, Time := as.POSIXct(Time, format="%Y%m%d_%H%M%S",tz="America/Los_Angeles")]
all_files[, Company := basename(dirname(V4))]
all_files <- all_files[!(Company == "coin_toss" & Time >= as.POSIXct("2022-02-13 18:12:21", tz="America/Los_Angeles"))]
all_files1 <- all_files[Time < as.POSIXct("2022-02-13 21:30:00", tz="America/Los_Angeles")]
setorder(all_files1, Company, -Time)
all_files1 <- all_files1[!(Company %in% "amc_plus")]
all_files1 <- all_files1[, .SD[1L], by=.(Company)]
all_files2 <- all_files[Time < as.POSIXct("2022-02-13 21:30:00", tz="America/Los_Angeles")]
setorder(all_files2, Company, -Time)
all_files2 <- all_files2[, .SD[1L], by=.(Company)]
all_files <- rbindlist(list(all_files1, all_files2), fill=TRUE)
setorder(all_files, Company,Time)
all_files <- all_files[, .SD[1L], by=.(Company)]
all_files <- all_files[, .SD[Time==max(Time)], by=.(Company)]

# # One of modifications

# List all files
commercial_data <- all_files[, .N, keyby=.(keyword=Company, File=V4)]

# Add on to list
Current_Files <- commercial_data[, .N, keyby=.(keyword,File)]

# Function
pull_data <- function(xPar) {
	print(xPar)
	Current_File <- Current_Files[keyword==xPar, File]
	dat <- read_parquet(paste0("s3://havas-data-science/",Current_File))
	setDT(dat)
	dat[, timestamp := as.POSIXct(date, tz="America/Los_Angeles")]
	this_date <- as.Date(dat[, max(timestamp)])
	if (this_date >= as.Date("2022-02-13")) {
		dat[, timestamp := timestamp - 5*60*60]
	}
	dat[, hits := as.character(hits)]
	dat[hits=="<1", hits := "0.5"]
	dat[, hits := as.numeric(hits)]
	out_plot <- dygraph(dat[, .(timestamp, hits)], main=xPar) %>% 
		dyOptions(useDataTimezone = TRUE)	
	#print(out_plot)
	dat[, File := Current_File]
	return(dat[])
}

# Loop and combine
all_dat <- lapply(Current_Files$keyword, pull_data)
all_dat <- rbindlist(all_dat, fill=TRUE)
all_dat[, keyword := as.character(keyword)]
all_dat[, keyword := tolower(keyword)]
#all_dat <- all_dat[keyword != "astronaunts"]

#Save data
saveRDS(all_dat, "commercial_data_2022.rds")
