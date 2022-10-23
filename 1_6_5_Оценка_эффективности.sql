-- модуль 1, урок 6, шаг 5
-- 1. Индексация в базах данных. - будет неэффективна, т.к записей оч. мало
-- 2. Профилирование: не прокатило.  При попытке выполнить SHOW PROFILES появилось сообщение:
--	 'SHOW PROFILES' is deprecated and will be removed in a future release. 
--	 Please use Performance Schema instead.
-- переконфигурировать сервер не рискнула
-- 3. Планы запроса с помощью EXPLAIN - удалось построить. но они более подробные в EXPLAIN ANALYZE
---------------------------------------------------------------------------------
-- ПЛАНЫ ЗАПРОСА
---------------------------------------------------------------------------------
/*Проанализированы 5 различных решений с использованием:


#774450583 - cte, TIMESTAMPDIFF, IN 								actual time=0.203..0.203, 	cost=16.94 
#502740449 - cte и ANY, DAYOFYEAR, YEARб  вложенный запрос			actual time=0.235..0.236, 	cost=3.65
#767139302 - cte, TIMESTAMPDIFF, вложенный запрос					actual time=0.221..0.247, 	cost=10.45
#733997109 - cte и ANY, TIMESTAMPDIFF, вложенный запрос 			actual time=0.517..0.519,   cost=3.65 
#782408367 - cte  и оконными функциями, TIMESTAMPDIFF   			actual time=45.721..45.722, cost=18.79

По времени выполнения - быстрее всего с cte и ANY. Использование DAYOFYEAR, YEAR вместо TIMESTAMPDIFF 
оказалось в 2 раза быстрее при сохранении энергоэфффективности.

По энергозатратам - наиболее эффективно оказалось cte + ANY.

Безусловный aутсайдер по времени и энергозатратам - cte + оконная ф-я
*/
---------------------------------------------------------------------------------
-- решение #774450583 - cte, TIMESTAMPDIFF, IN
---------------------------------------------------------------------------------
explain analyze
WITH sois(D)
AS(SELECT TIMESTAMPDIFF(YEAR,date_birth,'2021-08-07') FROM resume ORDER BY 1
LIMIT 3)
SELECT applicant, specialisation, position, D AS Возраст FROM resume, sois
WHERE  TIMESTAMPDIFF(YEAR,date_birth,'2021-08-07') IN(D)
ORDER BY 4, 1;

-> Sort: sois.D, `resume`.applicant  (actual time=0.203..0.203 rows=4 loops=1)
    -> Stream results  (cost=16.94 rows=102) (actual time=0.171..0.192 rows=4 loops=1)
        -> Inner hash join (timestampdiff(YEAR,`resume`.date_birth,'2021-08-07') = sois.D)  (cost=16.94 rows=102) (actual time=0.168..0.186 rows=4 loops=1)
            -> Table scan on resume  (cost=1.22 rows=34) (actual time=0.034..0.043 rows=34 loops=1)
            -> Hash
                -> Table scan on sois  (cost=0.85..2.54 rows=3) (actual time=0.000..0.001 rows=3 loops=1)
                    -> Materialize CTE sois  (cost=4.80..6.49 rows=3) (actual time=0.069..0.070 rows=3 loops=1)
                        -> Limit: 3 row(s)  (cost=3.65 rows=3) (actual time=0.057..0.057 rows=3 loops=1)
                            -> Sort: timestampdiff(YEAR,`resume`.date_birth,'2021-08-07'), limit input to 3 row(s) per chunk  (cost=3.65 rows=34) (actual time=0.057..0.057 rows=3 loops=1)
                                -> Table scan on resume  (cost=3.65 rows=34) (actual time=0.026..0.034 rows=34 loops=1)


----------------------------------------------------------------------------------------
explain WITH sois(D)
AS(SELECT TIMESTAMPDIFF(YEAR,date_birth,'2021-08-07') FROM resume ORDER BY 1
LIMIT 3)
SELECT applicant, specialisation, position, D AS Возраст FROM resume, sois
WHERE  TIMESTAMPDIFF(YEAR,date_birth,'2021-08-07') IN(D)
ORDER BY 4, 1;

-- Результат:
1	PRIMARY	<derived2>		ALL					3	100.0	Using temporary; Using filesort
1	PRIMARY	resume			ALL					34	100.0	Using where; Using join buffer (hash join)
2	DERIVED	resume			ALL					34	100.0	Using filesort
---------------------------------------------------------------------------------
--решение #767139302 - cte, вложенный запрос,TIMESTAMPDIFF
---------------------------------------------------------------------------------
explain analyze
WITH t1 (let)
AS (SELECT TIMESTAMPDIFF(YEAR, date_birth, '2021-08-07') AS let
    FROM resume
    ORDER BY let
    LIMIT 3)
    
SELECT applicant, specialisation, position, TIMESTAMPDIFF(YEAR, date_birth, '2021-08-07') AS Возраст
FROM resume
WHERE TIMESTAMPDIFF(YEAR, date_birth, '2021-08-07') IN (SELECT let FROM t1)
ORDER BY Возраст, applicant;

-> Nested loop inner join  (cost=10.45 rows=34) (actual time=0.221..0.247 rows=4 loops=1)
    -> Sort: timestampdiff(YEAR,`resume`.date_birth,'2021-08-07'), `resume`.applicant  (cost=3.65 rows=34) (actual time=0.156..0.159 rows=34 loops=1)
        -> Table scan on resume  (cost=3.65 rows=34) (actual time=0.085..0.096 rows=34 loops=1)
    -> Filter: (timestampdiff(YEAR,`resume`.date_birth,'2021-08-07') = `<subquery2>`.let)  (cost=1.17..1.17 rows=1) (actual time=0.002..0.002 rows=0 loops=34)
        -> Single-row index lookup on <subquery2> using <auto_distinct_key> (let=timestampdiff(YEAR,`resume`.date_birth,'2021-08-07'))  (actual time=0.000..0.000 rows=0 loops=34)
            -> Materialize with deduplication  (cost=6.79..6.79 rows=3) (actual time=0.076..0.077 rows=3 loops=1)
                -> Filter: (t1.let is not null)  (cost=4.80..6.49 rows=3) (actual time=0.053..0.054 rows=3 loops=1)
                    -> Table scan on t1  (cost=0.85..2.54 rows=3) (actual time=0.000..0.001 rows=3 loops=1)
                        -> Materialize CTE t1  (cost=4.80..6.49 rows=3) (actual time=0.053..0.053 rows=3 loops=1)
                            -> Limit: 3 row(s)  (cost=3.65 rows=3) (actual time=0.044..0.044 rows=3 loops=1)
                                -> Sort: timestampdiff(YEAR,`resume`.date_birth,'2021-08-07'), limit input to 3 row(s) per chunk  (cost=3.65 rows=34) (actual time=0.027..0.027 rows=3 loops=1)
                                    -> Table scan on resume  (cost=3.65 rows=34) (actual time=0.009..0.014 rows=34 loops=1)

----------------------------------------------------------------------------------
explain WITH t1 (let)
AS (SELECT TIMESTAMPDIFF(YEAR, date_birth, '2021-08-07') AS let
    FROM resume
    ORDER BY let
    LIMIT 3)
    
SELECT applicant, specialisation, position, TIMESTAMPDIFF(YEAR, date_birth, '2021-08-07') AS Возраст
FROM resume
WHERE TIMESTAMPDIFF(YEAR, date_birth, '2021-08-07') IN (SELECT let FROM t1)
ORDER BY Возраст, applicant;

-- результат: 
1	PRIMARY	resume													ALL					34	100.0	Using filesort
1	PRIMARY	<subquery2>		eq_ref	<auto_distinct_key>	<auto_distinct_key>	9	func	1	100.0	Using where
2	MATERIALIZED	<derived3>										ALL					3	100.0	
3	DERIVED	resume													ALL					34	100.0	Using filesort

---------------------------------------------------------------------------------
-- Решение #733997109 - cte и ANY, TIMESTAMPDIFF, вложенный запрос
---------------------------------------------------------------------------------
explain analyze
with cte as (
select    
    TIMESTAMPDIFF(year,  date_birth, '2021-08-07')   
from 
    resume
order by 1
limit 3)
select
    applicant,
    specialisation,
    position,
    TIMESTAMPDIFF(year, date_birth, '2021-08-07') as Возраст
from resume
where TIMESTAMPDIFF(year, date_birth,'2021-08-07')<= any(select * from cte)
order by Возраст, applicant;

-> Sort: timestampdiff(YEAR,`resume`.date_birth,'2021-08-07'), `resume`.applicant  (cost=3.65 rows=34) (actual time=0.517..0.519 rows=4 loops=1)
    -> Filter: <nop>((timestampdiff(YEAR,`resume`.date_birth,'2021-08-07') <= (select #2)))  (cost=3.65 rows=34) (actual time=0.389..0.470 rows=4 loops=1)
        -> Table scan on resume  (cost=3.65 rows=34) (actual time=0.095..0.129 rows=34 loops=1)
        -> Select #2 (subquery in condition; run only once)
            -> Aggregate: max(cte.`TIMESTAMPDIFF(year,  date_birth, '2021-08-07')`)  (cost=4.90..6.79 rows=3) (actual time=0.242..0.243 rows=1 loops=1)
                -> Table scan on cte  (cost=0.85..2.54 rows=3) (actual time=0.001..0.003 rows=3 loops=1)
                    -> Materialize CTE cte  (cost=4.80..6.49 rows=3) (actual time=0.225..0.228 rows=3 loops=1)
                        -> Limit: 3 row(s)  (cost=3.65 rows=3) (actual time=0.151..0.153 rows=3 loops=1)
                            -> Sort: timestampdiff(YEAR,`resume`.date_birth,'2021-08-07'), limit input to 3 row(s) per chunk  (cost=3.65 rows=34) (actual time=0.149..0.150 rows=3 loops=1)
                                -> Table scan on resume  (cost=3.65 rows=34) (actual time=0.041..0.074 rows=34 loops=1)
---------------------------------------------------------------------------------
EXPLAIN with cte(age) as(select TIMESTAMPDIFF(YEAR,  date_birth, '2021-08-07') from resume order by 1 limit 3)
select applicant, specialisation, position,  TIMESTAMPDIFF(YEAR, date_birth, '2021-08-07') as Возраст
from resume where TIMESTAMPDIFF(year, date_birth,'2021-08-07')<= any(select * from cte)
order by 4, 1;

-- Результат
1	PRIMARY	resume								ALL					34	100.0	Using where; Using filesort
2	SUBQUERY	<derived3>						ALL					3	100.0	
3	DERIVED	resume								ALL					34	100.0	Using filesort

---------------------------------------------------------------------------------
-- решение #502740449 с cte и ANY, DAYOFYEAR, YEAR, вложенный запрос
---------------------------------------------------------------------------------                         
explain analyze
WITH get_age_group(applicant, specialisation, position, Возраст)
AS(SELECT applicant, specialisation, position, IF (DAYOFYEAR(date_birth)<DAYOFYEAR('2021-08-07'), (YEAR('2021-08-07') - YEAR(date_birth)), (YEAR('2021-08-07') - YEAR(date_birth)-1)) AS Возраст
FROM resume),
get_limit(Возраст) AS (SELECT Возраст FROM get_age_group ORDER BY Возраст LIMIT 3)
SELECT* FROM get_age_group WHERE Возраст <= ANY(SELECT*FROM get_limit) ORDER BY Возраст, applicant;

-> Sort: if((dayofyear(`resume`.date_birth) < <cache>(dayofyear('2021-08-07'))),(<cache>(year('2021-08-07')) - year(`resume`.date_birth)),((<cache>(year('2021-08-07')) - year(`resume`.date_birth)) - 1)), `resume`.applicant  (cost=3.65 rows=34) (actual time=0.235..0.236 rows=4 loops=1)
    -> Filter: <nop>((if((dayofyear(`resume`.date_birth) < <cache>(dayofyear('2021-08-07'))),(<cache>(year('2021-08-07')) - year(`resume`.date_birth)),((<cache>(year('2021-08-07')) - year(`resume`.date_birth)) - 1)) <= (select #3)))  (cost=3.65 rows=34) (actual time=0.184..0.210 rows=4 loops=1)
        -> Table scan on resume  (cost=3.65 rows=34) (actual time=0.045..0.059 rows=34 loops=1)
        -> Select #3 (subquery in condition; run only once)
            -> Aggregate: max(get_limit.`Возраст`)  (cost=4.90..6.79 rows=3) (actual time=0.110..0.110 rows=1 loops=1)
                -> Table scan on get_limit  (cost=0.85..2.54 rows=3) (actual time=0.001..0.001 rows=3 loops=1)
                    -> Materialize CTE get_limit  (cost=4.80..6.49 rows=3) (actual time=0.098..0.098 rows=3 loops=1)
                        -> Limit: 3 row(s)  (cost=3.65 rows=3) (actual time=0.073..0.073 rows=3 loops=1)
                            -> Sort: if((dayofyear(`resume`.date_birth) < dayofyear('2021-08-07')),(year('2021-08-07') - year(`resume`.date_birth)),((year('2021-08-07') - year(`resume`.date_birth)) - 1)), limit input to 3 row(s) per chunk  (cost=3.65 rows=34) (actual time=0.072..0.072 rows=3 loops=1)
                                -> Table scan on resume  (cost=3.65 rows=34) (actual time=0.015..0.029 rows=34 loops=1)

---------------------------------------------------------------------------------
explain WITH get_age_group(applicant, specialisation, position, Возраст)
AS(SELECT applicant, specialisation, position, IF (DAYOFYEAR(date_birth)<DAYOFYEAR('2021-08-07'), (YEAR('2021-08-07') - YEAR(date_birth)), (YEAR('2021-08-07') - YEAR(date_birth)-1)) AS Возраст
FROM resume),
get_limit(Возраст) AS (SELECT Возраст FROM get_age_group ORDER BY Возраст LIMIT 3)
SELECT* FROM get_age_group WHERE Возраст <= ANY(SELECT*FROM get_limit) ORDER BY Возраст, applicant;

-- результат: 
1	PRIMARY	resume							ALL					34	100.0	Using where; Using filesort
3	SUBQUERY	<derived4>					ALL					3	100.0	
4	DERIVED	resume							ALL					34	100.0	Using filesort
---------------------------------------------------------------------------------
-- решение  #782408367 С cte  и оконными функциями, TIMESTAMPDIFF
---------------------------------------------------------------------------------

EXPLAIN ANALYZE
with get_list(resume_id,age,rn) as (
  select resume_id,timestampdiff(year,date_birth,'2021-08-07'), rank()over win_test
  from resume
window win_test as(order by timestampdiff(year, date_birth,'2021-08-07'))
)
  select applicant, specialisation, position, timestampdiff(year, date_birth,'2021-08-07') as Возраст
from resume
join get_list using(resume_id)
where rn<=3
order by 4 ,1;

---> Sort: `Возраст`, `resume`.applicant  (actual time=45.721..45.722 rows=4 loops=1)
    -> Stream results  (cost=18.79 rows=11) (actual time=45.092..45.681 rows=4 loops=1)
        -> Nested loop inner join  (cost=18.79 rows=11) (actual time=45.079..45.662 rows=4 loops=1)
            -> Filter: (get_list.rn <= 3)  (cost=0.56..6.33 rows=11) (actual time=45.032..45.566 rows=4 loops=1)
                -> Table scan on get_list  (cost=2.50..2.50 rows=0) (actual time=0.001..0.020 rows=34 loops=1)
                    -> Materialize CTE get_list  (cost=2.50..2.50 rows=0) (actual time=45.029..45.050 rows=34 loops=1)
                        -> Window aggregate: rank() OVER win_test  (actual time=44.921..44.937 rows=34 loops=1)
                            -> Sort: timestampdiff(YEAR,`resume`.date_birth,'2021-08-07')  (cost=4.40 rows=34) (actual time=44.914..44.915 rows=34 loops=1)
                                -> Table scan on resume  (cost=4.40 rows=34) (actual time=44.859..44.870 rows=34 loops=1)
            -> Single-row index lookup on resume using PRIMARY (resume_id=get_list.resume_id)  (cost=1.01 rows=1) (actual time=0.014..0.014 rows=1 loops=4)
---------------------------------------------------------------------------------
explain with get_list(resume_id,age,rn) as (
  select resume_id,timestampdiff(year,date_birth,'2021-08-07'), rank()over win_test
  from resume
window win_test as(order by timestampdiff(year, date_birth,'2021-08-07'))
)
  select applicant, specialisation, position, timestampdiff(year, date_birth,'2021-08-07') as Возраст
from resume
join get_list using(resume_id)
where rn<=3
order by 4 ,1;
-- результат: 
1	PRIMARY	<derived2>							ALL					34	33.33	Using where; Using temporary; Using filesort
1	PRIMARY	resume	eq_ref	PRIMARY	PRIMARY	4	get_list.resume_id	1	100.0	
2	DERIVED	resume								ALL					34	100.0	Using filesort
--------------------------------------------------------------------------------




