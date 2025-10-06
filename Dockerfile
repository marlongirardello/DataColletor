# ==============================================================================
# Dockerfile para o Data Collector Bot
# ==============================================================================

# Etapa 1: Imagem Base
# Usamos uma imagem oficial do Python, na versão "slim", que é leve e otimizada.
FROM python:3.10-slim

# Etapa 2: Variáveis de Ambiente
# PYTHONUNBUFFERED garante que os 'prints' do script apareçam nos logs do Koyeb em tempo real.
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Etapa 3: Diretório de Trabalho
# Define o diretório de trabalho dentro do container.
WORKDIR /app

# Etapa 4: Instalação das Dependências
# Copia apenas o arquivo de requisitos primeiro. Isso otimiza o cache do Docker.
# Se você alterar seu script .py mas não as dependências, esta etapa não será executada novamente.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Etapa 5: Copiar o Código da Aplicação
# Copia o resto dos arquivos do projeto (o data_collector.py) para o container.
COPY . .

# Etapa 6: Comando de Execução
# Define o comando que será executado quando o container iniciar.
CMD ["python", "AutoCrypoMarlon.py"]
