query_escenario1 = """Select RESFET FECHAENVIO, RESHOT HORAENVIO,
RESOPR CODIGOOPERADOR,
SUBSTring(RE.RESTEB,3,10) TRANSACCIONEBANCA,
Digits(CU.CUSCUN) NUMCLIENTE , CU.CUSNA1 NOMBRECLIENTE,
Digits(RE.RESCTA) CUENTACARGO, RE.RESEFR CODIGOBANCODESTINO,
RE.RESREF CUENTADESTINO,
CAST(RE.RESAMT AS DECIMAL(14,2)) MONTO,
RESFEL FECHALIQUIDACION, RESHOL HORALIQUIDACION,
RE.RESTIP TIPO, RE.RESFL2 FLAGPENSIONADO,
RE.RESSTA ESTATUS, RE.RESCRE CODIGORECHAZO
From BACCYFILES.ACMST AS AC
Inner Join BACCYFILES.ACHRESFF As RE On AC.ACMACC = RE.RESCTA
Inner Join BACHIFILES.CUMST As CU On AC.ACMCUN = CU.CUSCUN limit 5000 """


insert_horario_escenario1 = """ Insert into ach_test.ach_extendido_horario1(FECHAENVIO,HORARIOENVIO,CODIGOOPERADOR,
TRANSACCIONEBANCA,NUMCLIENTE,NOMBRECLIENTE,CUENTACARGO,CODIGOBANCODESTINO,
CUENTADESTINO,MONTO,FECHALIQUIDACION,HORALIQUIDACION,TIPO,FLAGPENSIONADO,ESTATUS,CODIGORECHAZO)
values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""