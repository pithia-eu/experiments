CREATE DEFINER=`root`@`localhost` PROCEDURE `storm_proc`()
BEGIN
	DECLARE done INT DEFAULT 0;
     DECLARE minMAG,minSTORM,maxSTORM,startSCAN datetime;

     DECLARE StormT_PREV,BzT_PREV,StormT_ITER,BzT_ITER,EndT_ITER,EndT_PREV datetime;

     DECLARE StormCUR CURSOR FOR
     select ACE_date as Storm_date,min(Bz_date) as Bz_date from (
     select * from
     (
         select aa.ACE_date ACE_date,aa.Bmag Bmag,(aa.Bmag-ab.Bmag) dBmag from
         magdata as aa
         join
         ( select ADDTIME(ACE_date,'01:00:00') ACE_date,Bmag from magdata) as ab
         ON aa.ACE_date=ab.ACE_date
         HAVING dBmag>=0 AND (dBmag>3.8 OR aa.Bmag>=13)
         ORDER BY ACE_date asc
     ) as a
     join
     (
         select ba.ACE_date Bz_date from
         ( select ACE_date,Bz from magdata where Bz<-10) as ba
         join
         ( select ADDTIME(ACE_date,'-01:00:00') as ACE_date,Bz from magdata where Bz<-10) as bb
         ON ba.ACE_date=bb.ACE_date
         order by Bz_date asc
     ) as b
     ON b.Bz_date>=a.ACE_date AND b.Bz_date<=ADDTIME(a.ACE_date,'03:00:00')
     ORDER BY ACE_date asc, Bz_date asc
     ) as c
     group by ACE_date;
     DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

     SET minMAG=(select min(ACE_date) from magdata where flg_scstrm=0);
     SET minSTORM=(select min(Storm_Onset) from stormsace where isclosed=0);
     SET maxSTORM=(select max(Storm_Onset) from stormsace where isclosed=1);
     IF minSTORM IS NULL AND maxSTORM IS NULL THEN
            SET startSCAN=(select min(ACE_date) from magdata);
     ELSEIF minSTORM IS NULL AND maxSTORM IS NOT NULL THEN
            SET startSCAN=minMAG;
     ELSE
            SET startSCAN=LEAST(minMAG,minSTORM);
     END IF;

     set StormT_PREV=startSCAN;

     OPEN StormCUR;
     StormCUR_loop: LOOP
           FETCH StormCUR INTO StormT_ITER,BzT_ITER;
           IF done
              THEN LEAVE StormCUR_loop;
           END IF;
           set EndT_PREV=(select Storm_Offset from stormsace
                          where Storm_Onset<StormT_ITER AND Storm_Offset is not NULL
                          order by Storm_Onset desc LIMIT 1);

           IF TIMESTAMPDIFF(HOUR,StormT_PREV,StormT_ITER)>3 AND
           (
              (EndT_PREV is NOT NULL AND StormT_ITER>=EndT_PREV) OR
              (EndT_PREV is NULL AND (select count(*) from stormsace)=0)
           )
           THEN
              set EndT_ITER=(select min(ACE_date) from magdata where ACE_date>BzT_ITER AND Bz>-1);
              insert into stormsace (Storm_Onset,Bz_minTime,Storm_Offset,isclosed) VALUES (StormT_ITER,BzT_ITER,EndT_ITER,(SELECT EndT_ITER IS NOT NULL))
              ON DUPLICATE KEY UPDATE `Bz_minTime`=VALUES(`Bz_minTime`),`Storm_Offset`=VALUES(`Storm_Offset`),`isclosed`=VALUES(`isclosed`);
              set StormT_PREV=StormT_ITER;
           END IF;
     END LOOP StormCUR_loop;
     CLOSE StormCUR;
     SET maxSTORM=(select max(Storm_Onset) from stormsace where isclosed=1);
     update magdata set flg_scstrm=1 where ACE_date<=maxSTORM AND ACE_date>=startSCAN;
END;