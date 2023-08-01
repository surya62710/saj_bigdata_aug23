/*select number,
SELECT hook_type, 
   CASE hook_type
      WHEN 0 THEN 'OFFER'
      WHEN 1 THEN 'ACCEPT'
      WHEN 2 THEN 'EXPIRED'
   END AS hook_name,
   COUNT(*) AS number_of_exchange_activities 
FROM `exchange` 
GROUP BY hook_type*/

/*1.create atable with 5 columns(id,id_0.id_1,id_1,id_2,id_3)
2.Insert data 
3.if id_0 orid_1 or id_2 or id_3 ==permit cat 1
3.if id_0 orid_1 or id_2 or id_3 ==restrict cat2
both cat3.
if starts with p= cat1
else ifstarts with r
============================================*/
solution

with cat as  (SELECT *, if(id_0 like 'p%' or id_1 like 'p%' or id_2 like 'p%',"cat1","cat2" )as catgeroy  FROM EMPLOYEE)
select empId,if (catgeroy='cat1' and (id_0 like 'r%' or id_1 like 'r%' or id_2 like 'r%'),"cat3",catgeroy) as category1  from cat;