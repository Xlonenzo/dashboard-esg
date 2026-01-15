@echo off
echo ================================================
echo    API ANBIMA ESG - Dashboard
echo ================================================
echo.
echo Verificando dependencias...
pip install flask flask-cors pyodbc -q
echo.
echo Iniciando servidor API em http://localhost:5000
echo.
echo Endpoints disponiveis:
echo   GET /api/health       - Status da API
echo   GET /api/fundos       - Lista fundos com paginacao
echo   GET /api/fundos/stats - Estatisticas gerais
echo.
echo Pressione Ctrl+C para parar o servidor
echo ================================================
echo.
python app.py
pause
