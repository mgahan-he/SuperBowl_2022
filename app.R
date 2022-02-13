
# Bring in libraries
library(data.table)
library(shiny)
library(dygraphs)

# Read in data
commercial_data <- readRDS("./commercial_data_2022.rds")

# Get specific files
# Current_File="Super_Bowl/capillus/CSVs/capillus_20190203_135249.csv"
# dat1 <- fread(cmd=paste0("aws s3 --profile scott cp s3://havas-data-science/",Current_File," -"))
# dat1[, timestamp := as.POSIXct(date, tz="America/Los_Angeles")]
# dat1[, hits := as.character(hits)]
# dat1[hits=="<1", hits := "0.5"]
# dat1[, hits := as.numeric(hits)]
# out_plot <- dygraph(dat1[, .(timestamp, hits)], main="Search") %>% 
# 	dyOptions(useDataTimezone = TRUE)	
# print(out_plot)
# dat1[, File := Current_File]

# Inputs
keywords <- commercial_data[, .N, keyby=.(keyword)]$keyword

# Build ui
ui <- fluidPage(
  titlePanel("Superbowl 2022 Commercials"),

  # Create a new Row in the UI for selectInputs
  fluidRow(
    column(4,
        selectInput("keyword_par","Google Keyword:",keywords, "the weeknd")
    )
  ),
	
  # Create a new row for the table.
  dygraphOutput("spikeplot")
)

# Build server
server <- function(input, output) {

  # Filter data based on selections
  output$spikeplot <- renderDygraph({
		
  	# Subset data
  	commercial_data_key <- commercial_data[keyword==input$keyword_par, .(timestamp, hits)]
  	
  	# Build plot
  	outplot <- dygraph(commercial_data_key, main=paste0("Google Search Intensity: ", input$keyword_par)) %>%
  		dyOptions(colors="darkred", strokeWidth = 2, fillGraph = TRUE, fillAlpha = 0.4, useDataTimezone = TRUE)
		
  	outplot
  })
}

# Run app
app_out <- shinyApp(ui = ui, server = server)
app_out