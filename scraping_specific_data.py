import snscrape.modules.twitter as sntwitter
import pandas as pd
import pymongo
import streamlit as st
import datetime
import time



st.write("# Twitter data scraping")
option = st.selectbox('How would you like the data to be searched?',('Keyword', 'Hashtag'))
word = st.text_input('Please enter a '+option, 'Example: LIC Policy')
start = st.date_input("Select the start date", datetime.date(2022, 1, 1),key='d1')
end = st.date_input("Select the end date", datetime.date(2023, 1, 1),key='d2')
tweet_c = st.slider('How many tweets to scrape', 0, 1000, 5)
tweets_list1 = []

# Using TwitterSearchScraper to scrape data and append tweets to list
for i,tweet in enumerate(sntwitter.TwitterSearchScraper('from:jack').get_items()): #declare a username 
    if i>tweet_c: #number of tweets you want to scrape
        break
    tweets_list1.append([tweet.date, tweet.id, tweet.url,tweet.content, tweet.user.username, tweet.replyCount, tweet.retweetCount,  tweet.source, tweet.likeCount]) #declare the attributes to be returned


    
# Creating a dataframe from the tweets list above 
tweets_df = pd.DataFrame(tweets_list1, columns=['Datetime', 'Tweet Id','Link' ,'Text', 'Username','Total replycount','Total retweetcount','Source', 'Total likecount'])
# tweets_df1.to_csv('user-tweets.csv', sep=',', index=False)

# Creating list to append tweet data to
tweets_list2 = []

# Using TwitterSearchScraper to scrape data and append tweets to list
for i,tweet in enumerate(sntwitter.TwitterSearchScraper('COVID Vaccine since:2021-01-01 until:2021-05-31').get_items()):
    if i>tweet_c:
        break
    tweets_list2.append([tweet.date, tweet.id, tweet.url,tweet.content, tweet.user.username, tweet.replyCount, tweet.retweetCount,  tweet.source, tweet.likeCount]) 
    
    
# Creating a dataframe from the tweets list above
tweets_df = pd.DataFrame(tweets_list2, columns=['Datetime', 'Tweet Id','Link' ,'Text', 'Username','Total replycount','Total retweetcount','Source', 'Total likecount'])
# tweets_df2.to_csv('text-query-tweets.csv', sep=',', index=False)
tweets_df.to_csv('text-query-tweets.csv', sep=',', index=False)


# Connecting python with mongodb
client= pymongo.MongoClient('mongodb://localhost:27017')
db=client['twitter_data']
mycollection= db['sample']

one_record= mycollection.find_one()
all_records= mycollection.find()
list_cursor= list(all_records)

df= pd.DataFrame(list_cursor)
@st.cache 
def convert_df(df):    
    return df.to_csv().encode('utf-8')

if not df.empty :
    csv = convert_df(tweets_df)
    st.download_button(label="Download data as CSV",data=csv,file_name='Twitter_data.csv',mime='text/csv',)
    
    
# DOWNLOAD AS JSON
    json_string = tweets_df.to_json(orient ='records')
    st.download_button(label="Download data as JSON",file_name="Twitter_data.json",mime="application/json",data=json_string,)    


# UPLOAD DATA TO DATABASE
    if st.button('Upload Tweets to Database'):
        coll=word
        coll=coll.replace(' ','_')+'_Tweets'
        mycoll=db[coll]
        dict=tweets_df.to_dict('records')
        if dict:
            mycoll.insert_many(dict) 
            ts = time.time()
            mycoll.update_many({}, {"$set": {"KeyWord_or_Hashtag": word+str(ts)}}, upsert=False, array_filters=None)
            st.success('Successfully uploaded to database', icon="✅")
            st.balloons()
        else:
            st.warning('Cant upload because there are no tweets', icon="⚠️")

    # SHOW TWEETS
    if st.button('Show Tweets'):
        st.write(tweets_df)

# SIDEBAR
with st.sidebar:   
    st.write('Uploaded Datasets: ')
    for i in db.list_collection_names():
        mycollection=db[i]
        #st.write(i, mycollection.count_documents({}))        
        if st.button(i):            
            dfm = pd.DataFrame(list(mycollection.find())) 

# DISPLAY THE DOCUMENTS IN THE SELECTED COLLECTION
if not dfm.empty: 
    st.write( len(dfm),'Records Found')
    st.write(dfm) 