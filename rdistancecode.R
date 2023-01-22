#distances
install.packages("rlist")
library(rlist)

install.packages("gmapsdistance")
library(gmapsdistance)

install.packages("geosphere")
library("geosphere")

library(dplyr)
#recode ppa levels for ppa link df
PPA <- recode(PPA_Level$`PPA Level`, "Program Participation Agreement - DTL & RDP" = "TL Groups", "Program Participation Agreement - RDP" = "Emerging Groups", "Program Participation Agreement - CRP, DTL & RDP" = "CRP/ROC")

NEWPPA <- cbind(PPA_Level, PPA)
View(NEWPPA)
write.csv(NEWPPA, "PPA Level Link.csv")
library(writexl)
write_xlsx(NEWPPA, "D:\\PPA_Level_Link.xlsx")






#code to add distances for shipping ytd report

mutate(df, error = df$`Origin Lat` - df$olat) -> df
geo_distance <- function(lon2, lon1, lat2, lat1){
  dlon <- lon2 - lon1
  dlat <- lat2 - lat1
  a <- (sin(dlat/2))^2 + cos(lat1) * cos(lat2) * (sin(dlon/2))^2
  c <- 2 * atan2(sqrt(a), sqrt(1-a))
  d <- 3961 * c
  d
}
library(dplyr)
pi <-3.14159265358979323846

df <- df %>% 
  mutate(distance = geo_distance(`Origin Long`*pi/180,`Dest Long`*pi/180,`Origin Lat`*pi/180,`Dest Lat`*pi/180))
library(readxl)





#add latitude and longitudes
# load the package
library(zipcodeR)
Good360_Shipping_YTD -> df
for (i in 1:nrow(df)) {
  zip <- df[i, 7]
  lat_long <- geocode_zip(zip)
  df$olat[i] <- lat_long$lat
  df$olong[i] <- lat_long$lng
}


#add latitude longitude to sales order df
zipcodelinktolatlong <- read.csv("~/zipcodelinktolatlong.csv")
View(zipcodelinktolatlong)
zipcodelinktolatlong[1] <- sapply(zipcodelinktolatlong[1], function(x) {
  sprintf("%05d", as.integer(x))
})
names(zipcodelinktolatlong)<- c('zip','Lat','Long')
df$`Shipping Zip`
sqldf(
  "SELECT df.*, z.Lat,z.Long
  FROM df
  LEFT JOIN zipcodelinktolatlong AS z
  ON z.zip = df.`Shipping Zip`
  "
)->df



#enforce zipcode formatting
df[6] <- sapply(df[6], function(x) {
  sprintf("%05d", as.integer(x))
})
#clean  unit column, add each for basic conservative estimate
df$Units <- ifelse(is.na(df$Units), "Each", df$Units)








#rename city field
rename(df, city = `Shipping City`) -> df

#add additional row key column
df <- df %>%
  mutate(ObservationNumber = row_number())


#add most common city field in order to replace missing city in observations

"SELECT ID, city, COUNT(city) as CityCount
                  FROM df
                  GROUP BY ID, city" -> subquery
sqldf(subquery) -> subquery
result <- sqldf("SELECT ID, city as MostCommonCity
                FROM subquery
                WHERE CityCount = (SELECT MAX(CityCount) FROM subquery s WHERE s.ID = subquery.ID)")
zresult <- sqldf("SELECT DISTINCT df.*, result.MostCommonCity
                FROM df
                LEFT JOIN result
                ON df.ID = result.id")
#remove rows which had been duplicated
df_clean <- df[!duplicated(df[, 1:17]), ]



df_clean -> df

#if no city is available for that observation, add the most frequently associated city with that org
df$city[is.na(df$city)] <- df$MostCommonCity[is.na(df$city)]

#clean city column
df <- df %>%
  # Make all the city names lower case
  mutate(city = tolower(city)) %>%
  # Remove any symbols or numbers from the city names, except spaces
  mutate(city = gsub("[^[:alpha:]\\ ]", "", city)) %>%
  # Remove leading and trailing whitespace from the city names
  mutate(city = trimws(city)) %>%
  # Capitalize the first letter of each city name
  mutate(city = gsub("\\b(.)", "\\U\\1", city, perl = TRUE))

#if city isnt na concatenate NPO name with city to create city specific NPO to differntiate locations
df <- df %>%
  mutate(NPO = ifelse(city != "Na", paste0(`Nonprofit Member`, " - ", city), `Nonprofit Member`))


#set non profit org name to NPO field where NPO is na
df$NPO[is.na(df$NPO)] <- df$`Nonprofit Member`[is.na(df$NPO)]


#add pallets
df$pallets <- ifelse(df$Units == "Each", df$Quantity / 100,
                     ifelse(df$Units == "Carton", df$Quantity / 75,
                            ifelse(df$Units == "Pallet", df$Quantity,
                                   ifelse(df$Units == "Gaylord", df$Quantity / 15,
                                          ifelse(df$Units == "Truckload", df$Quantity * 26, NA)))))
library(dplyr)
#add truckloads and estimated lives impacted
mutate(df, TLs = pallets / 26) -> df
mutate(df, Livesimpacted = TLs * 2.5* 1700) -> df

#addmost recent order date

qmax <- "SELECT NPO, MAX(Date) as MostRecentDate
FROM df
GROUP BY NPO;"
sqldf::sqldf(qmax) -> max

"SELECT df.*, max.MostRecentDate
FROM df
LEFT JOIN max
ON max.NPO = df.NPO" -> upq
sqldf::sqldf(upq) -> updateddf


updateddf -> df



library(sqldf)


#create state column so each NPO has a singular state
sqldf("SELECT NPO, state, COUNT(*) AS Frequency
FROM df
GROUP BY NPO, State
ORDER BY NPO, Frequency DESC;") -> states
states <- subset(states, state != "NA")

unique(states) -> states
sqldf("SELECT df.*, states.state
FROM df
LEFT JOIN states
ON df.NPO = states.NPO")->df2
df2 <- df2[!duplicated(df2[, "...1"]), ]





#create max occurring lat and long df in order to create a 1:1 for coordinates and NPO, used to link NPO to location
select(df2, NPO, Lat, Long) -> latilong
as.numeric(latilong$Long)-> latilong$Long
as.numeric(latilong$Lat)-> latilong$Lat
na.omit(latilong) -> latilong

qmaxl <- "SELECT NPO, MODE(Lat) as Maxlat
FROM latilong
GROUP BY NPO;"
sqldf::sqldf(qmaxl) -> maxlat

qmaxlo <- "SELECT NPO, MODE(Long) as Maxlong
FROM latilong
GROUP BY NPO;"
sqldf::sqldf(qmaxlo) -> maxlong

"SELECT maxlong.*, maxlat.Maxlat
FROM maxlong
LEFT JOIN maxlat
ON maxlat.NPO = maxlong.NPO" -> upqq
sqldf::sqldf(upqq) -> updateddf2
col_names <- c("NPO", "Lat", "Long")
names(updateddf2) <- col_names
write.csv(updateddf2, "Latlonglink.csv")
"SELECT updateddf2.*, maxlong.Maxlong
FROM updateddf2
LEFT JOIN maxlong
ON maxlong.ID = updateddf2.ID" -> upqq
sqldf::sqldf(upqq) -> updateddf2
updateddf2 -> df
df$Lat[is.na(df$Lat)] <- df$Maxlat[is.na(df$Lat)]
df$Long[is.na(df$Long)] <- df$Maxlong[is.na(df$Long)]




#fix additional uncleanliness
df$state <- ifelse(df$NPO == "Aid For Kids - Houlton", "MI", df$state)
df$state <- ifelse(df$NPO == "Cityserve Network - Dallas", "TX", df$state)
df$Region2 <- ifelse(df$NPO == "Cityserve Network - Dallas", "W", df$Region2)
df$Lat <- ifelse(df$NPO == "Cityserve Network - Dallas", 32.8785, df$Lat)
df$Long <- ifelse(df$NPO == "Cityserve Network - Dallas", -96.70782, df$Long)


#calculate total TL's
sum(df$TLs)
