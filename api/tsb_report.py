"""
Relatório TSB - Mapeamento Completo
"""
import pyodbc

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=ANBIMA_ESG;'
    'Trusted_Connection=yes;'
)
cursor = conn.cursor()

print('='*70)
print('TAXONOMIA SUSTENTAVEL BRASILEIRA (TSB) - MAPEAMENTO COMPLETO')
print('='*70)

cursor.execute('''
    SELECT e.SetorTSB, e.Emissor, e.Classificacao,
           COUNT(k.CodigoKPI) as KPIs,
           SUM(CASE WHEN k.Status = 'Verificado' THEN 1 ELSE 0 END) as Verificados
    FROM tsb.EmpresasTSB e
    LEFT JOIN tsb.KPIsEmpresa k ON e.EmpresaID = k.EmpresaID
    GROUP BY e.SetorTSB, e.Emissor, e.Classificacao
    ORDER BY e.SetorTSB, e.Emissor
''')

current_setor = ''
for row in cursor.fetchall():
    if row[0] != current_setor:
        current_setor = row[0]
        print(f'\n[{current_setor.upper()}]')

    class_icon = 'V' if row[2] == 'VERDE' else 'T'
    verif = f'{row[4]}/{row[3]}' if row[3] > 0 else '0'
    print(f'  [{class_icon}] {row[1][:45]:45} | {row[3]} KPIs ({verif} verif)')

print('\n' + '='*70)
print('LEGENDA: [V]=Verde [T]=Transicao | verif=dados de relatorios oficiais')
print('='*70)

# Stats finais
cursor.execute('''
    SELECT
        SUM(CASE WHEN Classificacao = 'VERDE' THEN 1 ELSE 0 END) as Verde,
        SUM(CASE WHEN Classificacao = 'TRANSICAO' THEN 1 ELSE 0 END) as Trans
    FROM tsb.EmpresasTSB
''')
r = cursor.fetchone()

cursor.execute("SELECT COUNT(*) FROM tsb.KPIsEmpresa WHERE Status = 'Verificado'")
verif = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM tsb.KPIsEmpresa')
total_kpis = cursor.fetchone()[0]

print(f'\nResumo: {r[0]} empresas VERDE, {r[1]} em TRANSICAO')
print(f'KPIs: {total_kpis} total ({verif} verificados, {total_kpis-verif} estimados)')

conn.close()
