#DR_SF_UGC_WEEKLY_R_POS_SENTIMENT

#############################################
######## Weekly Product Review POS table ####
#############################################
### Note: This is the weekly code that gets
###			appended to the base datatable
#############################################

#!/opt/R/4.0.2/bin/R
#### Connect to Snowflake #####
rm(list = ls())
options(java.parameters = "-Xmx5g")
require("RJDBC")
jdbcDriver <- JDBC(driverClass="",
                   classPath="")

con <- dbConnect(jdbcDriver, "", 
                 dbname="",user="", password="")


########################################
### Pull product review SF tables ######
########################################

require("data.table")
require("dplyr")

reviews=dbGetQuery(con,'SELECT a.MODERATION_STATUS_DESC,a.REVIEW_ID,a.PRODUCT,a.REVIEW_TITLE,a.REVIEW_TEXT,a.REVIEW_RATING_NBR FROM ACTIVITY.ACTIVITY.PRODUCT_REVIEW a LEFT JOIN DR_SF_UGC_PRODUCT_REVIEW_SENTIMENT_SCORE b ON a.REVIEW_ID=b.REVIEW_ID WHERE b.REVIEW_ID is null')
reviews$REVIEW_ID=as.numeric(reviews$REVIEW_ID)
reviews=reviews[which(reviews$MODERATION_STATUS_DESC=="APPROVED"),]
#save(reviews,file="reviews_append.RData")


#Load sentiment dictionary to use later
bing_sentiment=dbGetQuery(con,'SELECT * FROM AK_BING_SENTIMENT_DICTIONARY')


####################
#### clean data
####################

### Isolate review text and review title
simple=reviews[,c("REVIEW_ID","PRODUCT","REVIEW_TEXT")]
simple_title=reviews[,c("REVIEW_ID","PRODUCT","REVIEW_TITLE")]

#### Separate into sentences
require("tidyr")
simple_v1= separate_rows(simple, REVIEW_TEXT, sep="\\Q.\\E")
simple_v1=separate_rows(simple_v1, REVIEW_TEXT, sep="\\Q!\\E")
simple_v1=separate_rows(simple_v1, REVIEW_TEXT, sep="\\Q;\\E")

simple_title_v1= separate_rows(simple_title, REVIEW_TITLE, sep="\\Q.\\E")
simple_title_v1= separate_rows(simple_title_v1, REVIEW_TITLE, sep="\\Q!\\E")
simple_title_v1= separate_rows(simple_title_v1, REVIEW_TITLE, sep="\\Q;\\E")
colnames(simple_title_v1)[3]="REVIEW_TEXT"

#### combine into single dataset
reviews_clean=as.data.frame(rbind(simple_v1,simple_title_v1))

#### Lowecase all text
reviews_clean$REVIEW_TEXT=tolower(reviews_clean$REVIEW_TEXT)

# make wasn't=was not, can't=can not, etc..
reviews_clean$REVIEW_TEXT=gsub("wasn[\u2019']t", "was not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("mustn[\u2019']t", "must not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("couldn[\u2019']t", "could not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("cannot", "can not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("shouldn[\u2019']t", "should not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("shan[\u2019']t", "shall not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("wouldn[\u2019']t", "would not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("mustn[\u2019']t", "must not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("doesn[\u2019']t", "does not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("hadn[\u2019']t", "had not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("haven[\u2019']t", "have not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("hasn[\u2019']t", "has not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("weren[\u2019']t", "were not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("aren[\u2019']t", "are not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("isn[\u2019']t", "is not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("won[\u2019']t", "will not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("can[\u2019']t", "can not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("didn[\u2019']t", "did not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("don[\u2019']t", "do not", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("i[\u2019']m", "i am", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("[\u2019']ve", " have", reviews_clean$REVIEW_TEXT) 
reviews_clean$REVIEW_TEXT=gsub("[\u2019|']s", "", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("[\u2019']re", " are", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("[\u2019']ll", " will", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("it[\u2019']d", "it would", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("\r\n\r\n", " ", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("\t", " ", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("\r", " ", reviews_clean$REVIEW_TEXT)
reviews_clean$REVIEW_TEXT=gsub("\n", " ", reviews_clean$REVIEW_TEXT)

#save(reviews_clean,file="reviews_clean.RData")

#### Replace Negated phrases
#negation_mapping=read.csv("negation_mapping.csv")

negation_mapping=data.frame(
					phrase=c('no issues',
'not recommend',
'no complaints',
'not worth',
'no break',
'not leak',
'not disappoint',
'not super',
'not bother',
'not comfortable',
'not regret',
'not bad',
'no leaks',
'not waste',
'not easy',
'not hurt',
'not bulky',
'not fall',
'not disappointed',
'no hot',
'not worry',
'no issue',
'not trust',
'no doubt',
'not love',
'not lose',
'not ideal',
'without issue',
'no worries',
'no breaking',
'not break',
'not warm',
'no pain',
'not happy',
'not worn',
'no regrets',
'not durable',
'no trouble',
'not perfect',
'not cheap',
'not impressed',
'never worn',
'never failed',
'not hot',
'not worried',
'no discomfort',
'no luck',
'not recommended',
'not hard',
'not smell',
'not adjustable',
'not crazy',
'without worrying',
'not cold',
'no support',
'not uncomfortable',
'not itchy',
'without breaking',
'no damage',
'not complain',
'not overheat',
'not stink',
'not miss',
'no hassle',
'not secure',
'not loose',
'not strong',
'without pain',
'never hurt',
'not burn',
'without losing',
'no joke',
'not stiff',
'not suitable',
'no sore',
'nothing wrong',
'no leaking',
'not compatible',
'not interfere',
'no odor',
'not cool',
'not flattering',
'not terrible',
'not fun',
'not restrictive',
'not support',
'no smell',
'not difficult',
'not intuitive',
'not afford',
'not enjoy',
'nothing fancy',
'without fear',
'without worry',
'not chafe',
'not properly',
'not thrilled',
'no traction',
'not fail',
'not satisfied',
'not wrinkle',
'bad luck',
'no leakage',
'no mess',
'no weird',
'not damage',
'not rip',
'without spilling',
'no fuss',
'not reliable',
'not sag',
'not accurate',
'not protect',
'not ready',
'nothing bad',
'not restrict',
'without issues',
'never cold',
'never disappointed',
'never fails',
'not flimsy',
'not scratchy',
'never lost',
'not broken',
'not concerned',
'not freeze',
'not helpful',
'without burning',
'no blister',
'no fault',
'no soreness',
'not lost',
'not painful',
'not pinch',
'not scratch',
'not sore',
'never worry',
'no concerns',
'not soft',
'no loose',
'not pleased',
'never worried',
'no fun',
'not effective',
'not failed',
'not supportive',
'without fail',
'no bad',
'no wrinkles',
'not mess',
'not safe',
'not snag',
'without falling',
'never broke',
'not afraid',
'not irritate',
'not waterproof',
'not prevent',
'not protect',
'kept dry',
'water resistant',
'not water resistant',
'not terribly',
'not bothered',
'no cold',
'not bright',
'not sturdy',
'not itch',
'not impossible',
'no protection',
'no excuse',
'no nonsense',
'no worse',
'not stable',
'not sufficient',
'never leaks',
'no easy',
'never bothered',
'not free',
'not noisy',
'not kill',
'no fear',
'not consistent',
'not usable',
'not regretted',
'nothing worse',
'not clean',
'not correct',
'not fallen',
'not perfectly',
'not sticky',
'not sweaty',
'no noise',
'not drain',
'no bugs',
'not adequate',
'not horrible',
'no leak',
'not smooth',
'not complaining',
'not excessively',
'not flexible',
'not skinny',
'not amazing',
'not tough',
'never regretted',
'no cons',
'not comfy',
'not hinder',
'without discomfort',
'no scratches',
'not freezing',
'not abuse',
'not hug',
'no significant',
'not stain',
'not vent',
'not die',
'not uncomfortably',
'not expensive',
'not falling',
'not comfortably',
'not detract',
'not improve',
'nothing negative',
'not convenient',
'not knock',
'not sloppy',
'not recomend',
'without leaking',
'never disappoints',
'no loss',
'not damaged',
'without disturbing',
'never recommend',
'never fallen',
'no stink',
'no success',
'not pleasant',
'not loud',
'not rough',
'not inspire',
'not pretty',
'not roomy',
'never broken',
'no warmth',
'not clunky',
'not impede',
'not unusual',
'no sharp',
'not confident',
'not nice',
'never fell',
'never uncomfortable',
'not ripped',
'no snags',
'not fond',
'not hate',
'not boil',
'not compact',
'not inexpensive',
'not readily',
'not win',
'not doubt',
'without warning',
'never fail',
'not breaking',
'not leaking',
'not tired',
'no annoying',
'no complaint',
'not rigid',
'no troubles',
'not fancy',
'not greasy',
'not securely',
'no itch',
'not replaceable',
'without difficulty',
'no difficulty',
'not efficient',
'not fatigued',
'not fret',
'not ideally',
'not impair',
'not miserable',
'not quiet',
'not suffice',
'not trustworthy'),
					synonym=c('functional',
'dissatisfied',
'satisfied',
'dissatisfied',
'functional',
'functional',
'satisfied',
'bad',
'waste',
'uncomfortable',
'satisfied',
'good',
'functional',
'satisfied',
'difficult',
'unwounded',
'compact',
'steady',
'satisfied',
'tempid',
'confident',
'functional',
'distrust',
'positive',
'dislike',
'win',
'dissatisfied',
'functional',
'confident',
'functional',
'functional',
'cold',
'unwounded',
'dissatisfied',
'unused',
'satisfied',
'breaks',
'functional',
'okay',
'expensive',
'dissatisfied',
'unused',
'functional',
'tempid',
'confident',
'comfortable',
'dissatisfied',
'dissatisfied',
'easy',
'good',
'inflexible',
'moderate',
'confident',
'warm',
'unsupported',
'comfortable',
'comfortable',
'functional',
'undamaged',
'satisfied',
'comfortable',
'unscented',
'notice',
'easy',
'insecure',
'tight',
'weak',
'unwounded',
'unwounded',
'unwounded',
'unwasted',
'real',
'flexible',
'bad',
'unwounded',
'functional',
'functional',
'incompatible',
'satisfied',
'unscented',
'bad',
'unappealing',
'acceptable',
'terrible',
'comfortable',
'unsupportive',
'unscented',
'easy',
'difficult',
'expensive',
'dissatisfied',
'simple',
'confident',
'confident',
'comfortable',
'wrong',
'dissatisfied',
'slippery',
'functional',
'dissatisfied',
'smooth',
'bad',
'functional',
'clean',
'normal',
'functional',
'functional',
'functional',
'easy',
'unreliable',
'functional',
'wrong',
'unreliable',
'unready',
'okay',
'comfortable',
'functional',
'warm',
'satisfied',
'functional',
'sturdy',
'comfortable',
'satisfied',
'functional',
'confident',
'unfrozen',
'dissatisfied',
'safe',
'unwounded',
'functional',
'unwounded',
'found',
'unwounded',
'comfortable',
'comfortable',
'unwounded',
'confident',
'confident',
'uncomfortable',
'tight',
'dissatisfied',
'confident',
'bad',
'broken',
'functional',
'unsupportive',
'functional',
'good',
'smooth',
'clean',
'unsafe',
'functional',
'functional',
'functional',
'confident',
'comfortable',
'faulty',
'faulty',
'faulty',
'waterproof',
'waterproof',
'faulty',
'moderately',
'fine',
'tempid',
'dim',
'week',
'weak',
'possible',
'exposed',
'unacceptable',
'straightforward',
'okay',
'weak',
'insufficient',
'waterproof',
'hard',
'fine',
'constricted',
'good',
'live',
'confidence',
'inconsistent',
'unusable',
'glad',
'terrible',
'dirty',
'wrong',
'stable',
'imperfectly',
'smooth',
'fresh',
'good',
'clog',
'pest free',
'inadequate',
'okay',
'waterproof',
'rough',
'okay',
'moderately',
'stiff',
'wide',
'mediocre',
'easy',
'glad',
'positive',
'uncomfortable',
'accessible',
'comfortable',
'smooth',
'unfrozen',
'gentle',
'loose',
'minor',
'stainproof',
'stuffy',
'alive',
'comfortable',
'inexpensive',
'stable',
'uncomfortable',
'focus',
'detriment',
'positive',
'inconvenient',
'compliment',
'neat',
'dissatisfied',
'leakproof',
'satisfied',
'gain',
'pristine',
'respectful',
'dissatisfied',
'stable',
'pleasant',
'failure',
'unpleasant',
'quiet',
'smooth',
'uninspired',
'ugly',
'tight',
'resilient',
'defective',
'convenient',
'convenient',
'usual',
'dull',
'doubtful',
'bad',
'stable',
'comfortable',
'stable',
'stable',
'dislike',
'okay',
'defective',
'bulky',
'expensive',
'inaccessible',
'lose',
'confident',
'suddenly',
'reliable',
'stable',
'leakproof',
'awake',
'pleasant',
'good',
'flexible',
'good',
'common',
'clean',
'unsecure',
'good',
'irreplaceable',
'easily',
'easily',
'inefficient',
'energetic',
'unworried',
'bad',
'easily',
'okay',
'loud',
'insufficient',
'unreliable')
)

#install.packages("mgsub")
require("mgsub")
reviews_clean$REVIEW_TEXT=mgsub(reviews_clean$REVIEW_TEXT, negation_mapping$phrase, negation_mapping$synonym)


#### Remove Stop Words
# Load "Stop Words" from the tidytext package
stopwords=c("i",
"me",
"my",
"myself",
"we",
"our",
"ours",
"ourselves",
"you",
"your",
"yours",
"yourself",
"yourselves",
"he",
"him",
"his",
"himself",
"she",
"her",
"hers",
"herself",
"it",
"its",
"itself",
"they",
"them",
"their",
"theirs",
"themselves",
"what",
"which",
"who",
"whom",
"this",
"that",
"these",
"those",
"am",
"is",
"are",
"was",
"were",
"be",
"been",
"being",
"have",
"has",
"had",
"having",
"do",
"does",
"did",
"doing",
"would",
"should",
"could",
"ought",
"i'm",
"you're",
"he's",
"she's",
"it's",
"we're",
"they're",
"i've",
"you've",
"we've",
"they've",
"i'd",
"you'd",
"he'd",
"she'd",
"we'd",
"they'd",
"i'll",
"you'll",
"he'll",
"she'll",
"we'll",
"they'll",
"isn't",
"aren't",
"wasn't",
"weren't",
"hasn't",
"haven't",
"hadn't",
"doesn't",
"don't",
"didn't",
"won't",
"wouldn't",
"shan't",
"shouldn't",
"can't",
"couldn't",
"mustn't",
"let's",
"that's",
"who's",
"what's",
"here's",
"there's",
"when's",
"where's",
"why's",
"how's",
"a",
"an",
"the",
"and",
"but",
"if",
"or",
"because",
"as",
"until",
"while",
"of",
"at",
"by",
"for",
"with",
"about",
"against",
"between",
"into",
"through",
"during",
"before",
"after",
"above",
"below",
"to",
"from",
"up",
"down",
"in",
"out",
"on",
"off",
"over",
"under",
"again",
"further",
"then",
"once",
"here",
"there",
"when",
"where",
"why",
"how",
"all",
"any",
"both",
"each",
"few",
"more",
"other",
"some",
"such",
"only",
"own",
"same",
"so",
"than",
"will"
)

#remove stopwords from review text
reviews_clean$REVIEW_TEXT_stopwordsRemoved=gsub(paste0('\\b',stopwords, '\\b', collapse = '|'), "",reviews_clean$REVIEW_TEXT)
reviews_clean_nostopwords=reviews_clean
#save(reviews_clean_nostopwords,file="reviews_clean_nostopwords.RData")

#### Parts of Speech
#https://towardsdatascience.com/easy-text-analysis-on-abc-news-headlines-b434e6e3b5b8
#install.packages("udpipe")
require("udpipe")

#upload POS model
pos_model=udpipe_download_model(language="english")
english_model=udpipe_load_model(file='english-ewt-ud-2.5-191206.udpipe')

#annotate reviews with POS model
annotate=udpipe_annotate(english_model,reviews_clean_nostopwords$REVIEW_TEXT_stopwordsRemoved, doc_id = reviews_clean_nostopwords$REVIEW_ID)
#save(annotate,file="reviews_clean_nostopwords_annotate.RData")

pos=data.frame(annotate)
#save(pos,file="reviews_clean_nostopwords_pos.RData")

#get rid of rows that have nonsense / urls
require("data.table")
to_remove=which((pos$token %like% "\\Q/\\E") | (nchar(pos$token)>25) | (pos$upos=='PUNCT'))
pos_clean=pos[-(to_remove),]
#save(pos_clean,file="reviews_clean_nostopwords_pos_clean.RData")

##### Pause here to get sentiment before exporting POS table ###

#####################################
### Sentiment Score per Review ######
#####################################
#https://www.r-bloggers.com/2019/01/you-did-a-sentiment-analysis-with-tidytext-but-you-forgot-to-do-dependency-parsing-to-answer-why-is-something-positive-negative/
require("magrittr")
#require("tidytext")
require('dplyr')
load(file("https://github.com/sborms/sentometrics/raw/master/data-raw/valence-raw/valShifters.rda"))

#bing_sentiment is a Snowflake table from above

bing_sentiment$SENTIMENT= ifelse(bing_sentiment$SENTIMENT== "positive", 1, -1)
colnames(bing_sentiment)=c("term","polarity")
additions=data.frame(term=c("breathability","breathable","waterproof","allow","allowed","allows","compact"),polarity=c(1,1,1,1,1,1,1))
bing_sentiment=data.frame(rbind(bing_sentiment,additions))

polarity_negators=subset(valShifters$valence_en, t == 1)$x
polarity_negators=c(polarity_negators,"without")
polarity_amplifiers=subset(valShifters$valence_en, t == 2)$x
polarity_deamplifiers=subset(valShifters$valence_en, t == 3)$x

# requires 'udpipe'
sentiment=txt_sentiment(pos_clean, term = "lemma",
                        polarity_terms = bing_sentiment,
                        polarity_negators = polarity_negators, 
                        polarity_amplifiers = polarity_amplifiers,
                        polarity_deamplifiers = polarity_deamplifiers)
sentiment_scores=sentiment$data

sentiment_scores_parsed=sentiment_scores %>% 
  cbind_dependencies() %>%
  select(doc_id, lemma, token, upos, sentiment_polarity, token_parent, lemma_parent, upos_parent, dep_rel)

#save(sentiment_scores_parsed,file="sentiment_scores_full.RData")
sentiment_scores_parsed_limited=sentiment_scores_parsed[which(!is.na(sentiment_scores_parsed$sentiment_polarity)),]

# summarize POS sentiments into overal review sentiment value
avg_sentiment=sentiment_scores_parsed_limited %>%
  filter(upos %in% c("NOUN","ADJ","VERB","ADV")) %>%
  group_by(doc_id)%>%
  summarise(overall_sentiment = mean(sentiment_polarity))
avg_sentiment=as.data.frame(avg_sentiment)
colnames(avg_sentiment)=c("REVIEW_ID","RAW_SENTIMENT")
avg_sentiment$NORMALIZED_SENTIMENT=(avg_sentiment$RAW_SENTIMENT - min(avg_sentiment$RAW_SENTIMENT))/(max(avg_sentiment$RAW_SENTIMENT)-min(avg_sentiment$RAW_SENTIMENT))
avg_sentiment$NORMALIZED_SENTIMENT_V2=(-1) + (((avg_sentiment$RAW_SENTIMENT - min(avg_sentiment$RAW_SENTIMENT))*(1-(-1)))/(max(avg_sentiment$RAW_SENTIMENT)-min(avg_sentiment$RAW_SENTIMENT)))

#save(avg_sentiment,file="avg_sentiment_base_table.RData")


#Export Sentiment Table to upload to Snowflake
require("RJDBC")
jdbcDriver <- JDBC(driverClass="net.snowflake.client.jdbc.SnowflakeDriver",
                   classPath="/usr/local/app/snowflake/snowflake-jdbc-3.12.4.jar")

con <- dbConnect(jdbcDriver, "jdbc:snowflake://rei.us-west-2.privatelink.snowflakecomputing.com", 
                 dbname="DIGITAL_RETAIL_ANALYSIS",user="SVC_BI_DS_DR", password="eWWTr5wf.6XzKDLmjbBnC4Ba775.fBm37X")

dbSendQuery(con,'DROP TABLE IF EXISTS DIGITAL_RETAIL_ANALYSIS.DWADMIN.DR_SF_UGC_PRODUCT_REVIEW_SENTIMENT_SCORE_TEMP')
dbWriteTable(
  con,  #connection name as defined by dbConnect(jdbcDriver, ...)
  name='"DIGITAL_RETAIL_ANALYSIS"."DWADMIN"."DR_SF_UGC_PRODUCT_REVIEW_SENTIMENT_SCORE_TEMP"', #snowflake table name
  value=avg_sentiment #R dataframe name
)


#filter, merge with original data, save
colnames(pos_clean)[1]="REVIEW_ID"
pos_clean_limited=pos_clean[which(pos_clean$upos %in% c("ADJ","VERB","NOUN")),c("REVIEW_ID","token_id","token","lemma","upos")]
pos_clean_limited$upos=as.factor(pos_clean_limited$upos) #1="ADJ"  2="NOUN" 3="VERB"

to_merge=unique(reviews[,c("REVIEW_ID","PRODUCT","REVIEW_RATING_NBR")])
top_pos_part=merge(x=pos_clean_limited, y=to_merge, by="REVIEW_ID")
top_pos=merge(x=top_pos_part, y=avg_sentiment, by="REVIEW_ID")
str(top_pos)
top_pos$REVIEW_ID=as.numeric(top_pos$REVIEW_ID)
top_pos$NORMALIZED_SENTIMENT=round(top_pos$NORMALIZED_SENTIMENT,1)

# Aggregate to higher level
#	requires 'dplyr'
top_pos_agg = top_pos %>%
  group_by(PRODUCT,REVIEW_RATING_NBR,NORMALIZED_SENTIMENT,upos,lemma) %>%
  summarise(review_count = n_distinct(REVIEW_ID))

nrow(top_pos_agg)
sf_export=as.data.frame(top_pos_agg)
sf_export$review_count=as.numeric(sf_export$review_count)
colnames(sf_export)=c("PRODUCT","REVIEW_RATING_NBR","NORMALIZED_SENTIMENT","UPOS","LEMMA","REVIEW_COUNT")
#save(sf_export,file="reviews_clean_nostopwords_pos_export.RData")

#Export POS Table to upload to Snowflake
dbSendQuery(con,'DROP TABLE IF EXISTS DR_SF_UGC_PRODUCT_REVIEW_POS_TEMP')
dbWriteTable(
  con,  
  name='"DR_SF_UGC_PRODUCT_REVIEW_POS_TEMP"', 
  value=sf_export, #R dataframe name
  verbose=TRUE
)

# Disconnect
dbDisconnect(con)