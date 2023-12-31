

/*==================================================================

1. Create a schema based on the given dataset
2. Dump the data inside the hdfs in the given schema location.
3. List of all agents' names. 
4. Find out agent average rating.
5. Total working days for each agents 
6. Total query that each agent have taken 
7. Total Feedback that each agent have received 
8. Agent name who have average rating between 3.5 to 4 
9. Agent name who have rating less than 3.5 
10. Agent name who have rating more than 4.5 


================================================================
1.Create a schema based on the given dataset
2. Dump the data inside the hdfs in the given schema*/

create external table if not exists agent
     (SL_No int,
     doc string,
     Agent_Name string, 
     Total_Chats int,
     Average_Response_Time string,
     Average_Resolution_Time string,
     Average_Rating decimal,
     Total_Feedback int)
     row format delimited
     fields terminated by ','
     lines terminated by '\n'
     stored as textfile
     location '/user/bigdatacloudxlab39242/adithya/agent'
     tblproperties("skip.header.line.count"="1");
/*=================================================================
sl_no                   int                                         
doc                     string                                      
agent_name              string                                      
total_chats             int                                         
average_response_time   string                                      
average_resolution_time string                                      
average_rating          decimal(10,0)                               
total_feedback          int  
===========================================================*/
select agent_name
from agent
where sum(average_rating) between 3.5 and 4
group by agent_name


 


/*3. List of all agents' names. */

select Agent_Name 
    > from agent;
/*===============================================
4. Find out agent average rating.*/

select agent_name,avg(average_rating)
    > from agent
    > group by agent_name
    > ;
/*====================================================
5.hive> */
    > select agent_name,count(doc)
    > from agent
    > group by agent_name;
/*===================================================
6.hive> */
    > select agent_name,count(total_chats)
    > from agent
    > group by agent_name;


select agent_name,sum(total_feedback)
     from agent
    group by agent_name;

 select agent_name,average_rating
    > from agent
    > where average_rating > 4;