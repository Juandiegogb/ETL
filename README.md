# Consideraciones 


El usuario de Imperium BI no debe tener permiso de escritura en la base de datos del cliente


### notas

- tablero control de aplicaciones : consulta directa SELECT * FROM Hosvital.ApplicationControl en la base de datos Stage
- tablero control de citas : roto
- tablero agenda medica : roto
- tablero control de enfermeria : 
  - consulta directa a base de datos **Stage** en el schema **dbo**
  

    <details>
    <summary>Consulta</summary>
    <br>

    ```
    select 
    tenfermeria.*,
    TransactDevicesPivot.EncCod,
    TransactDevicesPivot.EncVer,
    TransactDevicesPivot.EncTip,
    TransactDevicesPivot.EncAgrDsc,
    TransactDevicesPivot.EncItmDsc,
    TransactDevicesPivot.ResFecHor,
    TransactDevicesPivot.[DISPOSITIVO MEDICO:],
    TransactDevicesPivot.[CALIBRE:],
    TransactDevicesPivot.[FECHA DE INSERCION:],
    TransactDevicesPivot.[FECHA DE SEGUIMIENTO:],
    TransactDevicesPivot.[FECHA DE RETIRO:],
    TransactDevicesPivot.[FECHA DE CURACION:],
    TransactDevicesPivot.[FECHA ACTUAL:],
    TransactDevicesPivot.[HORAS RETIRO:],
    TransactDevicesPivot.[SIGNOS Y SINTOMAS:],
    TransactDevicesPivot.[FLEBITIS:],
    TransactDevicesPivot.[SECRECIONES:],
    TransactDevicesPivot.[CULTIVOS BACTEREMIAS:],
    TransactDevicesPivot.[CULTIVOS SIN BACTEREMIAS:],
    TransactDevicesPivot.[EVENTOS ADVERSOS:],
    TransactDevicesPivot.[TIPO DE PROCEDIMIENTO:]
    from tenfermeria
    left join Hosvital.TransactDevicesPivot 
    on TransactDevicesPivot.ResTDoPac = tenfermeria.Tipo_Documento
    and TransactDevicesPivot.ResDocPac = tenfermeria.Numero_Documento
    and TransactDevicesPivot.EmpRazSoc COLLATE SQL_Latin1_General_CP1_CI_AS = tenfermeria.Nombre_razon_social
    and TransactDevicesPivot.ResFolPac = tenfermeria.HISCSEC
    --INNER JOIN DMHosvital.dbo.Users ON tenfermeria.EmpNit=Users.CompanyCode COLLATE SQL_Latin1_General_CP1_CI_AS
    --WHERE Users.AccountCode=@UserId
    ```

    </details>

    