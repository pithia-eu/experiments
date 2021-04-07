select a.id,a.ACE_date,a.Bmag,a.Bx,a.By,a.Bz,
CASE WHEN b.Storm_Onset THEN true ELSE FALSE END as flg_scstrm,
b.Storm_Onset,b.Storm_Offset,b.Bz_minTime,c.Bz as Bz_min
from magdata a left join stormsace b 
ON a.ACE_date>=b.Storm_Onset and (a.ACE_date<coalesce(b.Storm_Offset,@MaxCollatingDate))
left join magdata c ON b.Bz_minTime=c.ACE_date
having (a.Bx is not Null);