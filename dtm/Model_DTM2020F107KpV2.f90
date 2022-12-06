      program moddtm
!***********************************************************************
! s. bruinsma (22/11/22) 
!
! compile [double precision] dtm2020_F107_Kp-subr.f90 Model_DTM2020F107Kp.f90
!
  parameter (nlatm=96)
  common/cons/pi,deupi,cdr,sard
  common/cles/iklm
  dimension akp(4),fl(2),fm(2),texo(25),rop(25),talt(25)
  dimension o(25),az2(25),dens(25),par_ro(6),he(25)
  character*1 var_select
  pi=acos(-1.)
  deupi=2.*pi
  cdr=pi/180.
  pis12=pi/12.
  lim_lat=87
  latpas=3
!
  open(3,file='DTM20F107Kp_ro.datx',status='new')
  open(6,file='DTM20F107Kp_Tz.datx',status='new')
  open(8,file='DTM20F107Kp_Tinf.datx',status='new')
  open(11,file='DTM20F107Kp_He.datx',status='new')
  open(12,file='DTM20F107Kp_O.datx',status='new')
  open(13,file='DTM20F107Kp_N2.datx',status='new')
!
  open(10,file='DTM_2020_F107_Kp',status='old') 
  call lecdtm(10)	!read in coefficients
! *****************  input test values  ********************************
  alt=300.		!altitude
  day=180.		!day-of-year
  xlon=0.		!longitudfe
  fm(1)=180.	!mean F10.7 of last 81 days
  fm(2)=0.
  fl(1) =fm(1)	!F10.7 of previous day
  fl(2)=0.	  
  akp(1)=3.		!Kp delayed by three hours
  akp(2)=0.
  akp(3)=3.		!mean Kp of last 24 hours
  akp(4)=0.	
!
  write(*,*)'     Model: DTM2020_F107_Kp - density and partial densities in g/cm3'  
  write(*,*)
  write(*,*)'fm=',fm(1),'alt=',alt,'dayofyear=',day,'f=fm, 3-h kp=',akp(1),'24-h mean kp=',akp(3) 
  write(*,*)'use these variables; y(es) ou n(o) ?'
  read(*,2000) var_select
  if(var_select.eq.'n') then
    write(*,*)'fm,alt,dayofyear,f-fm,3-h kp, 24-h mean kp=? (integer)'
    read(*,*) ifm,ialti,iday,idf,iakp1,iakp3
    fm(1)=float(ifm)
    alt  =float(ialti)
    day  =float(iday)
    fl(1) =float(idf)+fm(1)
    akp(1)=float(iakp1)
    akp(3)=float(iakp3)
    write(*,*)'new variables: fm=',fm(1),'alt=',alt,'dayofyear=',day, &
    'f=',fl(1),'3-h kp=',akp(1), '24-h mean kp=',akp(3) 
  endif  
! *********************************************************************

do 100, i=-lim_lat,lim_lat,latpas
   alat=float(i)*cdr
   do 200, ihl=1,25
       hl=float(ihl-1)*pis12
       call dtm3(day,fl,fm,akp,alt,hl,alat,xlon,tz,tinf,ro,par_ro,wmm)
       texo(ihl) =tinf
        rop(ihl)  =ro
        talt(ihl) =tz
        az2(ihl)  =par_ro(4)
        o(ihl)    =par_ro(3)
        dens(ihl) =ro
        he(ihl)   =par_ro(2)
200  continue
      write(3,1000)  dens
      write(6,1000)  talt
      write(8,1000)  texo
      write(11,1000)  he
      write(12,1000)  o
      write(13,1000)  az2
100   continue
!
1000  format(25e15.4)
2000  format(a1)
end program moddtm
