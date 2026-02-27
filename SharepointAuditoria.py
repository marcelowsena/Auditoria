"""
UPLOAD SIMPLES PARA SHAREPOINT
Faz upload de arquivos já existentes para o SharePoint
Versão standalone - não depende do extrator Sienge
"""

import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
import time

from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext


def carregar_configuracao(config_path: str = 'sharepoint_config.json') -> dict:
    """Carrega configurações do SharePoint"""
    path = Path(config_path)
    
    if not path.exists():
        print(f"\n❌ ERRO: Arquivo {config_path} não encontrado!")
        print("\nCrie o arquivo com o seguinte formato:")
        print("""
{
    "site_url": "https://suaempresa.sharepoint.com/sites/SeuSite",
    "client_id": "seu-client-id-aqui",
    "client_secret": "seu-client-secret-aqui",
    "pasta_destino": "Documentos Compartilhados/Relatorios/Sienge"
}
        """)
        return {}
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        campos_obrigatorios = ['site_url', 'client_id', 'client_secret', 'pasta_destino']
        for campo in campos_obrigatorios:
            if campo not in config:
                print(f"❌ Campo obrigatório ausente: {campo}")
                return {}
        
        return config
    
    except json.JSONDecodeError as e:
        print(f"❌ Erro ao ler configuração: {e}")
        return {}


def autenticar_sharepoint(config: dict) -> Optional[ClientContext]:
    """Autentica no SharePoint"""
    try:
        credentials = ClientCredential(config['client_id'], config['client_secret'])
        ctx = ClientContext(config['site_url']).with_credentials(credentials)
        
        # Testar conexão
        web = ctx.web
        ctx.load(web)
        ctx.execute_query()
        
        print(f"✓ Conectado ao SharePoint: {web.properties['Title']}")
        return ctx
        
    except Exception as e:
        print(f"❌ Erro na autenticação: {e}")
        return None


def garantir_pasta_existe(ctx: ClientContext, caminho_pasta: str):
    """Garante que a estrutura de pastas existe"""
    try:
        pastas = caminho_pasta.split('/')
        caminho_atual = ""
        
        for pasta in pastas:
            if not pasta:
                continue
            
            caminho_atual = f"{caminho_atual}/{pasta}" if caminho_atual else pasta
            
            try:
                folder = ctx.web.get_folder_by_server_relative_url(caminho_atual)
                ctx.load(folder)
                ctx.execute_query()
            except Exception:
                parent = caminho_atual.rsplit('/', 1)[0] if '/' in caminho_atual else ""
                parent_folder = ctx.web.get_folder_by_server_relative_url(parent) if parent else ctx.web.root_folder
                parent_folder.folders.add(pasta)
                ctx.execute_query()
                print(f"  ✓ Pasta criada: {pasta}")
    
    except Exception as e:
        print(f"❌ Erro ao criar pasta: {e}")
        raise


def fazer_upload(ctx: ClientContext, arquivo_local: str, pasta_destino: str, 
                 nome_customizado: Optional[str] = None) -> bool:
    """
    Faz upload de um arquivo para o SharePoint
    
    Args:
        ctx: Contexto autenticado do SharePoint
        arquivo_local: Caminho completo do arquivo local
        pasta_destino: Pasta de destino no SharePoint
        nome_customizado: Nome opcional para o arquivo (se None, usa o nome original)
    
    Returns:
        True se sucesso, False se erro
    """
    try:
        caminho = Path(arquivo_local)
        
        if not caminho.exists():
            print(f"❌ Arquivo não encontrado: {arquivo_local}")
            return False
        
        nome_arquivo = nome_customizado if nome_customizado else caminho.name
        tamanho_mb = caminho.stat().st_size / 1024 / 1024
        
        print(f"\n📤 Fazendo upload...")
        print(f"   Arquivo: {caminho.name}")
        print(f"   Tamanho: {tamanho_mb:.2f} MB")
        print(f"   Destino: {pasta_destino}/{nome_arquivo}")
        
        # Garantir que a pasta existe
        garantir_pasta_existe(ctx, pasta_destino)
        
        # Ler arquivo
        with open(caminho, 'rb') as f:
            conteudo = f.read()
        
        # Upload
        target_folder = ctx.web.get_folder_by_server_relative_url(pasta_destino)
        inicio = time.time()
        target_folder.upload_file(nome_arquivo, conteudo).execute_query()
        tempo = time.time() - inicio
        
        print(f"   ✓ Upload concluído em {tempo:.1f}s")
        print(f"   📁 Arquivo disponível no SharePoint!")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro no upload: {e}")
        return False


def listar_arquivos(ctx: ClientContext, pasta_destino: str):
    """Lista arquivos na pasta de destino"""
    try:
        print(f"\n📂 Arquivos em {pasta_destino}:")
        
        folder = ctx.web.get_folder_by_server_relative_url(pasta_destino)
        files = folder.files
        ctx.load(files)
        ctx.execute_query()
        
        if not files:
            print("   (pasta vazia)")
            return
        
        # Ordenar por data de modificação (mais recentes primeiro)
        arquivos = sorted(
            files,
            key=lambda f: f.properties.get('TimeLastModified', ''),
            reverse=True
        )
        
        for f in arquivos[:10]:  # Mostrar últimos 10
            nome = f.properties.get('Name', 'Sem nome')
            tamanho = f.properties.get('Length', 0)
            
            # Converter para número se for string
            try:
                tamanho_mb = float(tamanho) / 1024 / 1024
            except (ValueError, TypeError):
                tamanho_mb = 0
            
            data = str(f.properties.get('TimeLastModified', ''))[:10]  # Apenas data
            print(f"   • {nome} ({tamanho_mb:.2f} MB) - {data}")
        
        if len(arquivos) > 10:
            print(f"   ... e mais {len(arquivos) - 10} arquivo(s)")
    
    except Exception as e:
        print(f"⚠ Erro ao listar arquivos: {e}")


def main():
    """Função principal"""
    print("="*70)
    print("UPLOAD SIMPLES PARA SHAREPOINT")
    print("="*70)
    
    # Processar argumentos
    if len(sys.argv) < 2:
        print("\n❌ ERRO: Nenhum arquivo especificado!")
        print("\nUso:")
        print("  python upload_simples.py arquivo.xlsx")
        print("  python upload_simples.py arquivo.xlsx nome_customizado.xlsx")
        print("  python upload_simples.py arquivo.xlsx nome_customizado.xlsx config.json")
        print("\nExemplos:")
        print("  python upload_simples.py Analise_Orcamento_Pedidos.xlsx")
        print("  python upload_simples.py relatorio.xlsx Relatorio_Final.xlsx")
        return False
    
    arquivo_local = sys.argv[1]
    nome_customizado = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2].endswith(('.xlsx', '.xls', '.csv', '.parquet', '.pdf')) else None
    config_file = sys.argv[3] if len(sys.argv) > 3 else (sys.argv[2] if len(sys.argv) > 2 and sys.argv[2].endswith('.json') else 'sharepoint_config.json')
    
    # Verificar se arquivo existe
    if not Path(arquivo_local).exists():
        print(f"\n❌ ERRO: Arquivo não encontrado: {arquivo_local}")
        
        # Tentar procurar arquivo similar
        pasta_atual = Path('.')
        nome_base = Path(arquivo_local).stem
        arquivos_similares = list(pasta_atual.glob(f"{nome_base}*"))
        
        if arquivos_similares:
            print(f"\n💡 Arquivos similares encontrados:")
            for arq in arquivos_similares[:5]:
                print(f"   • {arq.name}")
            print("\nTente novamente com o nome correto")
        
        return False
    
    # Carregar configuração
    print(f"\n[1/4] Carregando configuração de {config_file}...")
    config = carregar_configuracao(config_file)
    if not config:
        return False
    print("✓ Configuração carregada")
    
    # Autenticar
    print("\n[2/4] Autenticando no SharePoint...")
    ctx = autenticar_sharepoint(config)
    if not ctx:
        return False
    
    # Fazer upload
    print("\n[3/4] Fazendo upload do arquivo...")
    sucesso = fazer_upload(ctx, arquivo_local, config['pasta_destino'], nome_customizado)
    
    if not sucesso:
        return False
    
    # Listar arquivos (opcional)
    try:
        print("\n[4/4] Listando arquivos no SharePoint...")
        listar_arquivos(ctx, config['pasta_destino'])
    except Exception as e:
        print(f"   ⚠ Não foi possível listar arquivos: {e}")
    
    print("\n" + "="*70)
    print("✓ UPLOAD CONCLUÍDO COM SUCESSO!")
    print("="*70)
    print(f"Arquivo enviado para: {config['site_url']}")
    print(f"Pasta: {config['pasta_destino']}")
    print("="*70)
    
    return True


if __name__ == "__main__":
    try:
        sucesso = main()
        sys.exit(0 if sucesso else 1)
    except KeyboardInterrupt:
        print("\n\n⚠ Upload cancelado pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro inesperado: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)