--Проект

--Таблица для данных из файла group_log

DROP TABLE IF EXISTS STV2024050748__STAGING.group_log;

CREATE TABLE IF NOT EXISTS STV2024050748__STAGING.group_log(
group_id INT PRIMARY KEY,
user_id INT,
user_id_from INT,
event VARCHAR(10),
event_ts DATETIME
)
ORDER BY group_id, user_id
PARTITION BY event_ts::DATE
GROUP BY calendar_hierarchy_day(event_ts::DATE, 3, 2);


--Шаг 4. Создать таблицу связи l_user_group_activity

DROP TABLE IF EXISTS STV2024050748__DWH.l_user_group_activity;

CREATE TABLE IF NOT EXISTS STV2024050748__DWH.l_user_group_activity(
hk_l_user_group_activity INT PRIMARY KEY,
hk_user_id BIGINT NOT NULL CONSTRAINT fk_l_user_group_activity_user REFERENCES STV2024050748__DWH.h_users (hk_user_id),
hk_group_id  BIGINT NOT NULL CONSTRAINT fk_l_user_group_activity_group REFERENCES STV2024050748__DWH.h_groups(hk_group_id),
load_dt DATETIME,
load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_user_id ALL nodes --исправил здесь сегментирование 
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


--Шаг 5. Создать скрипты миграции в таблицу связи l_user_group_activity

INSERT INTO STV2024050748__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
SELECT DISTINCT
	    HASH(hg.group_id, hu.user_id),
        gl.group_id,
        gl.user_id,
        NOW() AS load_dt,
        's3' AS load_src
FROM STV2024050748__STAGING.group_log as gl
LEFT JOIN STV2024050748__DWH.h_users AS hu ON gl.user_id = hu.user_id 
LEFT JOIN STV2024050748__DWH.h_groups AS hg ON gl.group_id = hg.group_id;


--Шаг 6. Создать и наполнить сателлит s_auth_history

DROP TABLE IF EXISTS STV2024050748__DWH.s_auth_history;

CREATE TABLE IF NOT EXISTS STV2024050748__DWH.s_auth_history(
--Внешний ключ к ранее созданной таблице связей типа INT, потому что в сателлитах нет первичных ключей
hk_l_user_group_activity BIGINT NOT NULL CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES STV2024050748__DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from INT,
event VARCHAR(10),
event_ts DATETIME,
load_dt DATETIME,
load_src VARCHAR(20)
)
ORDER BY event_ts -- изменил сортировку с load_dt на event_ts
SEGMENTED BY hk_l_user_group_activity ALL nodes --правильно ли здесь сегментирование выполнено? 
PARTITION BY event_ts::DATE -- партиционирование 
GROUP BY calendar_hierarchy_day(event_ts::DATE, 3, 2); -- и группировку

--Наполнение сателлита s_auth_history данными из таблицы STV2024050748__STAGING.group_log

--Проверил сколько хэш-айдтшников в таблице hk_l_user_group_activity, чтобы понять, все ли попадают в таблицу
SELECT COUNT(DISTINCT hk_l_user_group_activity)--569 426
FROM STV2024050748__DWH.l_user_group_activity AS luga
WHERE luga.hk_l_user_group_activity IS NOT NULL;

SELECT COUNT(hk_l_user_group_activity)----569 426
FROM STV2024050748__DWH.l_user_group_activity AS luga;

--Вставка данных в таблицу s_auth_history
INSERT INTO STV2024050748__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_ts, load_dt, load_src)
SELECT DISTINCT luga.hk_l_user_group_activity,
	   			gl.user_id_from,
	   			gl.event,
	   			gl.event_ts,
	   			NOW() AS load_dt,
       			's3' AS load_src
FROM STV2024050748__DWH.l_user_group_activity AS luga
LEFT JOIN STV2024050748__STAGING.group_log AS gl ON luga.hk_l_user_group_activity = HASH(gl.group_id, gl.user_id);
;

--Шаг 7.1. Подготовить CTE user_group_messages
/*Я здесь считаю сколько юзеров в группах отправили хотя бы одно сообщение
 * Беру DISTINCT lum.hk_user_id из таблицы с сообщениями и пользователями, потому что, 
 * если они в этой таблце, значит отправили минимум одно сообщение.
 * */
WITH user_group_messages as (
SELECT lgd.hk_group_id AS hk_group_id,
       COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
FROM STV2024050748__DWH.l_groups_dialogs AS  lgd
LEFT JOIN STV2024050748__DWH.l_user_message AS lum ON lgd.hk_message_id = lum.hk_message_id
GROUP BY lgd.hk_group_id
)
SELECT hk_group_id,
            cnt_users_in_group_with_messages
FROM user_group_messages
ORDER BY cnt_users_in_group_with_messages
LIMIT 10
;


--Шаг 7.2. Подготовить CTE user_group_log


SELECT COUNT(1)
FROM STV2024050748__DWH.s_auth_history
WHERE event = 'add'; 

WITH user_group_log AS (
SELECT hg.hk_group_id AS hk_group_id,
	   COUNT(luga.hk_user_id) AS cnt_added_users -- 800 746
FROM STV2024050748__DWH.s_auth_history AS sah 
LEFT JOIN STV2024050748__DWH.l_user_group_activity AS luga ON sah.hk_l_user_group_activity = luga.hk_l_user_group_activity 
LEFT JOIN STV2024050748__DWH.h_groups AS hg ON sah.load_src = hg.load_src
WHERE 1=1 
AND hg.hk_group_id IN (SELECT hg.hk_group_id  --самые ранние группы
						 FROM STV2024050748__DWH.h_groups hg 
						 ORDER BY hg.registration_dt 
						 LIMIT 10)
AND sah.event = 'add'	
GROUP BY hg.hk_group_id
)
SELECT hk_group_id,
       cnt_added_users
FROM user_group_log
ORDER BY cnt_added_users
LIMIT 10
; 


--Шаг 7.3. Написать запрос и ответить на вопрос бизнеса

WITH 
user_group_messages AS (
SELECT lgd.hk_group_id AS hk_group_id,
       COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
FROM STV2024050748__DWH.l_groups_dialogs AS  lgd
LEFT JOIN STV2024050748__DWH.l_user_message AS lum ON lgd.hk_message_id = lum.hk_message_id
GROUP BY lgd.hk_group_id
),
user_group_log AS (
SELECT hg.hk_group_id AS hk_group_id,
	   COUNT(luga.hk_user_id) AS cnt_added_users -- 800 746
FROM STV2024050748__DWH.s_auth_history AS sah 
LEFT JOIN STV2024050748__DWH.l_user_group_activity AS luga ON sah.hk_l_user_group_activity = luga.hk_l_user_group_activity 
LEFT JOIN STV2024050748__DWH.h_groups AS hg ON sah.load_src = hg.load_src
WHERE 1=1 
AND hg.hk_group_id IN (SELECT hg.hk_group_id  --самые ранние группы
						 FROM STV2024050748__DWH.h_groups hg 
						 ORDER BY hg.registration_dt 
						 LIMIT 10)
AND sah.event = 'add'	
GROUP BY hg.hk_group_id
)
SELECT ugl.hk_group_id AS hk_group_id,
       ugl.cnt_added_users AS cnt_added_users,
       ugm.cnt_users_in_group_with_messages AS cnt_users_in_group_with_messages,
       ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users AS group_conversion
FROM user_group_log AS ugl 
LEFT JOIN user_group_messages AS ugm ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users DESC;


           
