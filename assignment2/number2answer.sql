select to_char(t.ts, 'YYYY-MM') AS month, count(distinct c.userid)  
from raw_data.session_timestamp as t 
inner join raw_data.user_session_channel as c
on t.sessionid = c.sessionid 
group by to_char(t.ts, 'YYYY-MM') 
order by 1 ;

