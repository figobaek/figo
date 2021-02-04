

[assignment1]

-- 테이블 만들기 
create table goldenboyy0524.practice (
	value int null
	);
	
-- 널 삽입
insert into goldenboyy0524.practice select null as value;

-- 널 삽입 가능여부 확인
select is_nullable, *
from information_schema.columns
where table_schema = 'goldenboyy0524' 
and table_name = 'practice'
order by table_name;

-- 컬럼 추가
alter table goldenboyy0524.practice add column play, varchar(255);

-- 컬럼에 값 삽입(컬럼 두 개 한 번에, 형 맞춰서)
insert into goldenboyy0524.practice select 1, 'ihi' as play;
insert into goldenboyy0524.practice values (2, '2');

-- 컬럼 명 변경
alter table goldenboyy0524.practice rename value to test;

-- 테이블 명 변경
alter table goldenboyy0524.practice rename to practica;
alter table goldenboyy0524.practice rename to practica;
select * from practica;
alter table goldenboyy0524.practica rename to practice;

-- count 메소드 연습
select count(distinct test) from practice;
select count(*) from practice;
select count(1) from practice;


-- (31 페이지) 채널 테이블 생성 연습, 순서대로 
create table goldenboyy0524.channel(
	channel varchar(32) primary key
	);
	
insert into goldenboyy0524.channel values ('facebook'), ('google');

select * from channel;


-- (31 페이지) 채널 테이블 생성 연습, 한 번에(CTAS)
drop table channel; 

create table goldenboyy0524.channel as 
select distinct channel
from raw_data.user_session_channel;

select * from channel;

-- (31 페이지) 채널 테이블 컬럼명 변경

alter table goldenboyy0524.channel rename channel to channelname;

-- (31 페이지) 값 추가 (틱톡)

insert into goldenboyy0524.channel values('TIKTOK');


-- ilike
select count(*) from raw_data.user_session_channel where channel ilike '%oo%';
select count(*) from raw_data.user_session_channel where channel not ilike '%oo%';

-- null value ordering

insert into goldenboyy0524.channel values(null);
select channelname from channel order by 1 nulls last ;
select channelname from channel order by 1 nulls first ;

-- convert timezone
select convert_timezone('America/Los_Angeles', ts), ts
from raw_data.session_timestamp;


-- excercise - session이 가장 많이 생성되는 시간대

select a.e, a.c, b.e, b.c
from (select extract(hour from ts) e,count(sessionid) c
from  raw_data.session_timestamp
group by 1
order by 2 desc
limit 1) a full join
(select extract(dow from ts) e, count(sessionid) c
from  raw_data.session_timestamp
group by 1
order by 2 desc
limit 1) b
on a.e = b.e;

-- 15시, 토요일

select extract(hour from ts) hours, count(distinct sessionid) countsession from raw_data.session_timestamp group by 1 order by 2 desc;

 
-- 채널별 사용자수 left join

select r.channel, count(u.sessionid)
from raw_data.channel r left join raw_data.user_session_channel u
on r.channel = u.channel
group by r.channel
order by 2 desc;

-- is this true == is this not false? null?  연습

create table nt (
	ntest boolean
	);
	
insert into nt select null as ntest;
insert into nt select true as ntest;
insert into nt select false as ntest;

select * from nt where ntest is not false; -- null도 같이 나온다.
select * from nt where ntest is true; -- true만 나온다. 
-- 다르다!! 


-- 조건부 delete

delete from goldenboyy0524.channel where channelname = 'Google';
select * from channel;

-- 53페이지 연습 1_251번 유저의 가장 처음과 마지막 사용 채널

select a.userid, a.channel, a.n
from (select userid, ts, channel, row_number() over(partition by userid order by ts) n 
from raw_data.user_session_channel r join raw_data.session_timestamp t
on r.sessionid = t.sessionid) a
where a.userid = 251 and a.n = 1
union
select a.userid, a.channel, a.n
from (select userid, ts, channel, row_number() over(partition by userid order by ts) n 
from raw_data.user_session_channel r join raw_data.session_timestamp t
on r.sessionid = t.sessionid) a
where a.userid = 251 and a.n = (select count(userid) from raw_data.user_session_channel where userid = 251);

-- 53페이지 연습 2_모든 유저의 가장 처음과 마지막 사용 채널

select a.userid, a.channel, a.seqe
from (SELECT userid, ts, channel, row_number() over(partition by userid order by ts desc) seqe
FROM raw_data.user_session_channel usc
JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid) a
where a.seqe = 1
union
select a.userid, a.channel, a.seqe 
from (SELECT userid, ts, channel, row_number() over(partition by userid order by ts desc) seqe
FROM raw_data.user_session_channel usc
JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid) a join (select userid, count(userid) ct from raw_data.user_session_channel group by userid) b 
on a.userid = b.userid and a.seqe = b.ct
order by 1, 3;

-- Subquery, with 연습 ( 해당 쿼리 안에서만 존재하는 가상 임시 테이블)

with ch as (select distinct channel from raw_data.user_session_channel)
select * from ch;






[assignment2] - / Gross Revenue rank 10 / 

select u.userid, sum(t.amount)
from raw_data.user_session_channel u left join raw_data.session_transaction t
on u.sessionid = t.sessionid
group by 1
order by 2 desc nulls last
limit 10;





[assignment3] 


채널별 월 매출액 테이블 만들기 (본인 스키마 밑에 CTAS로 테이블 만들기)
session_timestamp, user_session_channel, channel, session_transaction 테이블들을 사용
channel에 있는 모든 채널에 대해 구성해야함 (값이 없는 경우라도)
아래와 같은 필드로 구성
month
channel
uniqueUsers (총방문 사용자)
paidUsers (구매 사용자: refund한 경우도 판매로 고려)
conversionRate (구매사용자 / 총방문 사용자)
grossRevenue (refund 포함)
netRevenue (refund 제외)

-- 다시 하나씩 쪼개서 

-- 방문 고객 수 
select count(distinct userid) from raw_data.user_session_channel;

-- 구매 고객 수 
select count(distinct sessionid) from raw_data.session_transaction;

-- 월별 방문 수 

select extract(month from ts), count(r.sessionid)
from raw_data.session_timestamp r
group by 1;

-- 월별 방문 고객 수, 구매 고객 수, 구매량. 틱톡을 넣어줘야 하니까 마지막에 추가 


create table goldenboyy0524.assn2draft as 
select extract(month from r.ts) as month ,ch.channel as channel, convert(float, count(u.sessionid)) uniqueUser, convert(float, count(t.sessionid)) paidUsers, (Case when paidUsers = 0 or uniqueUser = 0 then null Else (paidUsers / uniqueUser) * 100||'%' end) as conversionRate,  sum(t.amount) grossRevenue, sum(rv.namount) netRevenue
from raw_data.session_timestamp r join raw_data.session_transaction t
on r.sessionid = t.sessionid join raw_data.user_session_channel u
on t.sessionid = u.sessionid join (select sessionid, (case when refunded is TRUE then 0 else amount end) namount from raw_data.session_transaction) rv 
on u.sessionid = rv.sessionid right join raw_data.channel ch on u.channel = ch.channel
group by 1, 2
order by 1, 2;

select * from assn2draft;


--해결 또는 이해 못 한 것
-- 1. 월별로 나뉘어지면 유니크한 고객 구분이 어려워지는데 그럼 매 월 다른 고객으로 간주해야 하는 건지
-- 2. 그렇다고 가정할 때 코드를 어떻게 짜야 할 지 감을 잡을 수가 없음. 만들어놓은 결과도 이상함. 


-- 세션이 달라도 한 고객이 여러번 구매한 것일 수 있으므로. 그것을 풀어야 한다. 

-- 방문 고객 수 - 유니크한 방문 고객 수 949, 고로 구매 전환율 합계는 534/949
select count(distinct userid) from raw_data.user_session_channel;

-- 구매 고객 수 - 유니크한 고객 수 534명. 
select count(sessionid) from raw_data.session_transaction;

-- with문으로 재시도, 실패

with b1(mn, ch, us, si) as
(	select extract('month' from ti.ts), 
	ch.channel, 
	ur.userid, ti.sessionid
from raw_data.channel ch left join raw_data.user_session_channel ur
on ch.channel = ur.channel join raw_data.session_timestamp ti
on ur.sessionid = ti.sessionid),
b2 as 
(  select b1.ch, 
	b1.mn, 
	count(distinct b1.us) as uniqueUser
from b1
group by 1, 2
order by 1, 2),
b3 as
(	select b1.ch,
	b1.mn,
	count(distinct b1.us) as paidUser
from b1 join raw_data.session_transaction tr
on b1.si = tr.sessionid
group by 1, 2
order by 1, 2
	 )
select b1.mn, b1.ch, b2.uniqueUser, b3.paidUser
FROM b1, b2, b3;





