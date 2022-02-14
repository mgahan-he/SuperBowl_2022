
# Bring in libraries
library(data.table)
library(gtrendsR)
suppressMessages(library(utils))
suppressMessages(suppressPackageStartupMessages(library(arrow,quietly=TRUE)))
setwd("/home/mgahan/SuperBowl_2022")

# Key companies
key_companies <- c("austin powers","scarjost","planet fitness","lindsay lohan","zac taylor","national anthem",
	"Mickey Guyton","Dr Dre", "Snoop Dogg","Eminem","Mary J Blige","Kendrick Lamar","sofi","Sean McVay",
	"Matthew Stafford","Megan Thee Stallion","Zendaya","Squarespace","André 3000","She Sells Sea Shells","alexa",
	"joe burrow","nebraska","coin toss","billie jean king","5 planes","national anthem length",
	"jurassic park","the rock","toyota","dolly parton","tmobile","miley cyrus","rakuten","expedia",
	"law and order","pepsi","ewan mcgregor","bose","odell beckham jr","bud light next","draft kings",
	"arnold schwarzenegger","selma hayek","bmw","peacock","griffith park","netflix","the adam project",
	"polestart","avocados of mexico","carvana","hologic","coinbase","qr code","telemundo",
	"fifa world cup","doritos","questys","meta quest","nope","dirk vermeil","al michaels","cris collinsworth",
	"bad hold","steve buscemi","peyton manning","serena williams","michelob","silverado","the sopranos",
	"raheem morris","joe mixon","anna kendricks","barbie","rocket homes","disney plus",
	"weather tech","wrestlemania","hollywood sign","alex morgan","burrows mom","do it for the phones",
	"uber eats","gwetheth paltrow","cooper manning","mannings","ambulance","intuit","wallbox",
	"turkish airlines","morgan freeman","brooks koepka","jay-z","lebron","emmitt smith","jennifer lopez","matt damon",
	"ben affleck","taunting","roman numerals","salesforce","matthew mcconaughey","ftx","larry david","joe vs carole",
	"chobani","pechanga","monday","hug project","jones","jonas","bel air","omaha","tams burgers","50 Cent",
	"Anderson Paak","typical nfl halftime length","5G UW","crypto.com", "young Lebron","Jim Carrey","The Cable Guy",
	"Marry Me","Ted Danzen","Keely Clarkson","Keenan","The Botanist","Offensive Pass Inference","google pixel",
	"william shatner","whodey","dr evil","general motors","Etora","The Rings of Power","Paud Rudd","Seth Rogan","Lays","Scrubs",
	"Pringles","guy fieri","robo dog","Kia EV6","Stafford","Von Miller","clickup","shaq","black rifle",
	"gigillionaire","burrow hurt","brito apparel","booking.com","lit","mvp","most times sacked",
	"cutwater spirits","vroom","amc plus","irish spring","monobob","the thing about pam",
	"eugeen levy","nissan","taco bell","jerrod mayo","pete davidson","best foods",
	"russell wlison","Roger Goodell","clydesdale","kevin hart","sams club","tytanic","greenlight","ty burrell",
	"clayton kershaw","stan kroenke","andrew whitworth","aaron donald","belair","michele tafoya","sketchers","Willie Nelson",
	"Cooper Kupp","maria taylor","mike tirico")
key_companies <- c("Cooper Kupp","maria taylor","mike tirico","grubhub","draftkings","mvp")
key_companies <- c("alexa","austin powers","scarjost","planet fitness","lindsay lohan","zac taylor",
	"Dr Dre", "Snoop Dogg","Eminem","Mary J Blige","Kendrick Lamar","law and order","Idris Elba")
key_companies = c("do it for the phones","maria taylor","mike tirico","monday","monday.com","monobob","most times sacked",
	"mvp","nebraska","pepsi","robo dog","sketchers","stan kroenke","ted danson","the thing about pam","tmobile","t mobile",
	"toyota","vroom","Willie Nelson")

key_companies <- tolower(key_companies)
key_companies <- unique(key_companies)
key_companies <- rev(key_companies)

# Read in data
retrieve_data_func <- function(company_par) {
	
	# Current time company_par
	print(company_par)
	Current_Time <- Sys.time()
	Current_Time <- gsub("\\s+","_",gsub("[-:]","",Current_Time))
	
	# Update Company name
	Company_Name <- gsub("\\s+", "_", company_par)
	
	# Create outfiles
	out_rds <- paste0(Company_Name,"_",Current_Time,".rds")
	out_csv <- paste0(Company_Name,"_",Current_Time,".csv")
	
	# Pull data
	dat_list <- gtrends(company_par, time = "now 4-H", geo = "US") 
	
	# Organize data
	dat <- as.data.table(dat_list$interest_over_time)
	dat[, date := date-3*60*60]
	dat[, date := as.character(date)]
	#fwrite(dat, out_csv)
	saveRDS(dat_list, out_rds)
	
	# Upload to S3
	upload_txt_rds <- paste0("aws s3 mv ", out_rds, " s3://havas-data-science/Super_Bowl/SB_22/",Company_Name,"/Lists/",out_rds)
	upload_txt_csv <- paste0("aws s3 mv ", out_csv, " s3://havas-data-science/Super_Bowl/SB_22/",Company_Name,"/CSVs/",out_csv)
	upload_sys_rds <- system(upload_txt_rds, intern=TRUE)
	#upload_sys_csv <- system(upload_txt_csv, intern=TRUE)
	upload_txt_parquet <- paste0("s3://havas-data-science/Super_Bowl/SB_22/Parquet/",Company_Name,"/",Current_Time,".parquet")
	write_parquet(dat, upload_txt_parquet)
	
	# Return output
	return(company_par)
	
}

# Error handling
retrieve_data_trycatch <- function(company_par_iter) {
	output_attempt <- tryCatch(
		{
			retrieve_data_func(company_par=company_par_iter)
		},
		error=function(cond) {
			return(paste0(company_par_iter," had error"))
		},
		warning=function(cond) {
		},
		finally={
			return(paste0(company_par_iter," had error"))
		}
	)
	return(output_attempt)
}


# Create loop
for (xComp  in key_companies) {
		retrieve_data_trycatch(company_par_iter=xComp)
		Sys.sleep(30)
}
print(Sys.time())

# Asis schedule
# */45 * * * *
# dat <- read_parquet("s3://havas-data-science/Super_Bowl/SB_21/Parquet/uber_eats/20210206_224942.parquet")
# dat[, timestamp := as.POSIXct(date, format="%Y-%m-%d %H:%M:%S")]
# library(dygraphs)
# dygraph(dat[, .(timestamp, hits)])