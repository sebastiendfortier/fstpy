! S/P FSTCOMPSTATS  COMPARAISON DE DEUX CHAMPS REELS DE UNE DIMENSION

      SUBROUTINE FSTCOMPSTATS(A, B, N, ERRRELMAX, ERRRELMOY, VARA, VARB, CCOR, MOYA, MOYB, BIAS, ERRMAX, ERRMOY)

      IMPLICIT NONE
      INTEGER,  intent(in) :: N
      REAL,  intent(in)    :: A(N)
      REAL,  intent(in)    :: B(N)
      REAL*8, intent(out)  :: ERRRELMAX
      REAL*8, intent(out)  :: ERRRELMOY
      REAL*8, intent(out)  :: VARA
      REAL*8, intent(out)  :: VARB
      REAL*8, intent(out)  :: CCOR
      REAL*8, intent(out)  :: MOYA
      REAL*8, intent(out)  :: MOYB
      REAL*8, intent(out)  :: BIAS
      REAL*8, intent(out)  :: ERRMAX
      REAL*8, intent(out)  :: ERRMOY
      ! ERRRELMAX(E-REL-MAX)), ERRRELMOY(E-REL-MO), VARA(VAR-A), VARB(VAR-B), CCOR(C-COR), MOYA(MOY-A), MOYB(MOY-B), MOYB-MOYA(BIAS), ERRMAX(E-MAX), ERRMOY(E-MOY)

      REAL*8 ERRABS

      ! AUTEURS  VERSION ORIGINALE (REALCMP)  M.VALIN DRPN 1987
      !          VERSION (FSTCOMPSTATS)  Y.BOURASSA DRPN JAN 1990
      !          Ajout de l'argument exception - M. Lepine Mars 2014

      ! ARGUMENTS
      ! ENTRE  A,B     CHAMPS REELS A COMPARER
      !    "    N       DIMENSION DE A ET B NI*NJ
      !                A UTILISER POUR DETERMINER SI "A" COMPARE A "B"


      INTEGER   I, irange
      REAL*8    SA, SB, SA2, SB2, DERR, AA, BB, FN
      REAL MIN_A, MAX_A, MIN_B, MAX_B, RANGE_A, RANGE_B
      ! REAL ratio_max, ratio
      ! integer nbdiff

      ! nbdiff = 0
      SA     = 0.
      SB     = 0.
      CCOR    = 0.
      SA2    = 0.
      SB2    = 0.
      ERRRELMAX = 0.
      ERRRELMOY    = 0.
      ERRMOY = 0.
      ERRMAX = 0.
   !   ratio_max = 0.
      MIN_A = A(1)
      MAX_A = A(1)
      MIN_B = B(1)
      MAX_B = B(1)
      DO 10 I=1,N
         AA     = A(I)
         BB     = B(I)
         MIN_A = MIN(MIN_A,A(I))
         MAX_A = MAX(MAX_A,A(I))
         MIN_B = MIN(MIN_B,B(I))
         MAX_B = MAX(MAX_B,B(I))
         SA     = SA+AA
         SB     = SB+BB
         IF(AA .NE. BB) THEN
         !   if (aa .ne. 0.) ratio = (max(aa,bb) - min(aa,bb)) / aa * 100
         !   if (ratio > ratio_max) ratio_max = ratio

         !   nbdiff = nbdiff +1

            ERRABS = ABS(AA-BB)
            ERRMOY = ERRMOY+ERRABS
            ERRMAX = MAX(ERRABS,ERRMAX)
            derr=0.0
            IF(AA .NE. 0.) THEN
               DERR = ABS(1.-BB/AA)
            ELSEIF(BB .NE. 0.)THEN
               DERR = ABS(1.-AA/BB)
            ENDIF
            ERRRELMAX = MAX(ERRRELMAX,DERR)
            ERRRELMOY    = ERRRELMOY+DERR
         ENDIF
   10    CONTINUE


      RANGE_A = MAX_A - MIN_A
      RANGE_B = MAX_B - MIN_B
      irange = TRANSFER(RANGE_A,1)
      irange = ISHFT(ISHFT(irange,-23) +1,23)
      RANGE_A = TRANSFER(irange,1.0)
      FN   = FLOAT(N)
      ERRRELMOY  = ERRRELMOY/FN
      MOYA = SA/FN
      MOYB = SB/FN
      DO 20 I=1,N
         AA  = A(I)-MOYA
         BB  = B(I)-MOYB
         CCOR = CCOR+AA*BB
         SA2 = SA2+AA*AA
   20    SB2 = SB2+BB*BB
      ERRMOY = ERRMOY/FN
      VARA   = SA2/FN
      VARB   = SB2/FN

      IF(SA2*SB2 .NE. 0.) THEN
         CCOR    = CCOR/SQRT(SA2*SB2)

      ELSEIF(SA2.EQ.0. .AND. SB2.EQ.0.) THEN
         CCOR = 1.0
      ELSEIF(SA2 .EQ. 0.) THEN
         CCOR = SQRT(VARB)
      ELSE
         CCOR = SQRT(VARA)
      ENDIF

      BIAS = MOYB-MOYA
   !   ERRRELMAX(E-REL-MAX)), ERRRELMOY(E-REL-MO), VARA(VAR-A), VARB(VAR-B), CCOR(C-COR), MOYA(MOY-A), MOYB(MOY-B), MOYB-MOYA(BIAS), ERRMAX(E-MAX), ERRMOY(E-MOY)


      RETURN
      END
