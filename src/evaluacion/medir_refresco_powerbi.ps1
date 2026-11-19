# Mide el tiempo de actualización completa del modelo de Power BI Desktop para cada tamaño.
# Uso (PowerShell, con el proyecto abierto en Power BI Desktop):
#   .\src\evaluacion\medir_refresco_powerbi.ps1 -Salida resultados\tiempos\powerbi_actualizacion.csv
# Cambia el parámetro Conjunto con TOM, mide el refresco completo (TMSL) y al final deja n100000.
param([string]$Salida, [int]$Repeticiones = 3, [int[]]$Tamanos = @(1000, 10000, 100000, 1000000))
$m = Get-Process msmdsrv | Select-Object -First 1
$port = (Get-NetTCPConnection -OwningProcess $m.Id -State Listen | Select-Object -First 1).LocalPort
Add-Type -Path (Join-Path (Split-Path $m.Path) 'Microsoft.PowerBI.AdomdClient.dll')
$c = New-Object Microsoft.AnalysisServices.AdomdClient.AdomdConnection("Data Source=localhost:$port")
$c.Open()
function Escalar($q) { $cmd = $c.CreateCommand(); $cmd.CommandText = $q; $r = $cmd.ExecuteReader(); $null = $r.Read(); $v = $r.GetValue(0); $r.Close(); $v }
function Ejecutar($tmsl) { $cmd = $c.CreateCommand(); $cmd.CommandText = $tmsl; $null = $cmd.ExecuteNonQuery() }
$db = Escalar 'SELECT [CATALOG_NAME] FROM $SYSTEM.DBSCHEMA_CATALOGS'
"Base de datos: $db (puerto $port)"

$bin = Split-Path $m.Path
Add-Type -Path "$bin\Microsoft.PowerBI.Amo.Core.dll"; Add-Type -Path "$bin\Microsoft.PowerBI.Tabular.dll"
$srv = New-Object Microsoft.AnalysisServices.Tabular.Server
$srv.Connect("Data Source=localhost:$port")
$tom = $srv.Databases[0]
function PonerConjunto($n) {
    # Solo cambia el parámetro (sin cargar datos); la carga se mide aparte con el refresco TMSL.
    $tom.Model.Expressions['Conjunto'].Expression = '"n' + $n + '" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]'
    $null = $tom.Model.SaveChanges()
}
$refresco = (@{ refresh = @{ type = 'full'; objects = @(@{ database = $db }) } } | ConvertTo-Json -Depth 6)

$filas = @('repeticion,n,segundos,tickets_cargados,inicio')
for ($k = 1; $k -le $Repeticiones; $k++) {
    foreach ($n in $Tamanos) {
        PonerConjunto $n
        $inicio = (Get-Date).ToString('s')
        $sw = [Diagnostics.Stopwatch]::StartNew()
        Ejecutar $refresco
        $sw.Stop()
        $filas_t = Escalar 'EVALUATE ROW("n", COUNTROWS(fact_tickets))'
        $s = [math]::Round($sw.Elapsed.TotalSeconds, 3).ToString([Globalization.CultureInfo]::InvariantCulture)
        "r$k n$n : $s s ($filas_t tickets)"
        $filas += "$k,$n,$s,$filas_t,$inicio"
        $filas | Set-Content -Encoding utf8 $Salida
    }
}
PonerConjunto 100000
Ejecutar $refresco
"Restaurado n100000: $(Escalar 'EVALUATE ROW("n", COUNTROWS(fact_tickets))') tickets"
$c.Close(); $srv.Disconnect()
