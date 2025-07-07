import sys
import os
import heapq

DIRETORIO_TEMP = "temp_files"

def gerar_runs_iniciais(arquivo_entrada, p):
    if not os.path.exists(DIRETORIO_TEMP):
        os.makedirs(DIRETORIO_TEMP)

    for f in os.listdir(DIRETORIO_TEMP):
        os.remove(os.path.join(DIRETORIO_TEMP, f))

    num_registros = 0
    nomes_dos_runs = []
    try:
        with open(arquivo_entrada, 'r') as f_in:
            heap = []
            numeros_congelados = []
            run_atual_num = 0

            for _ in range(p):
                linha = f_in.readline()
                if not linha:
                    break
                heapq.heappush(heap, int(linha.strip()))
                num_registros += 1

            while heap or numeros_congelados:
                if not heap:
                    heap = numeros_congelados
                    numeros_congelados = []
                    heapq.heapify(heap)

                run_atual_num += 1
                nome_run = os.path.join(DIRETORIO_TEMP, f"run_{run_atual_num}.tmp")
                nomes_dos_runs.append(nome_run)
                print(f"Criando run: {nome_run}")

                with open(nome_run, 'w') as f_run:
                    while heap:
                        menor_valor = heapq.heappop(heap)
                        f_run.write(str(menor_valor) + '\n')
                        ultimo_escrito = menor_valor

                        proxima_linha = f_in.readline()
                        if not proxima_linha:
                            continue
                        
                        num_registros += 1
                        proximo_valor = int(proxima_linha.strip())

                        if proximo_valor >= ultimo_escrito:
                            heapq.heappush(heap, proximo_valor)
                        else:
                            numeros_congelados.append(proximo_valor)
            
            while True:
                linha = f_in.readline()
                if not linha:
                    break
                num_registros +=1
    
    except FileNotFoundError:
        print(f"Erro: Arquivo de entrada '{arquivo_entrada}' não encontrado.")
        return 0, []
        
    print(f"Total de {len(nomes_dos_runs)} runs iniciais geradas.")
    return num_registros, nomes_dos_runs

def intercalar_runs(runs_de_entrada, arquivo_de_saida):
    heap = []
    arquivos_abertos = []

    for i, nome_run in enumerate(runs_de_entrada):
        try:
            f = open(nome_run, 'r')
            arquivos_abertos.append(f)
            primeira_linha = f.readline()
            if primeira_linha:
                valor = int(primeira_linha.strip())
                heapq.heappush(heap, (valor, i))
        except IOError as e:
            print(f"Erro ao abrir o arquivo de run {nome_run}: {e}")
            continue

    with open(arquivo_de_saida, 'w') as f_out:
        while heap:
            menor_valor, indice_arquivo = heapq.heappop(heap)
            f_out.write(str(menor_valor) + '\n')

            proxima_linha = arquivos_abertos[indice_arquivo].readline()
            if proxima_linha:
                proximo_valor = int(proxima_linha.strip())
                heapq.heappush(heap, (proximo_valor, indice_arquivo))

    for f in arquivos_abertos:
        f.close()

def main():
    if len(sys.argv) != 4:
        print("Erro: Número incorreto de parâmetros.")
        return

    try:
        p_ways = int(sys.argv[1])
        if p_ways < 2:
            print("Erro: O valor de 'p' deve ser 2 ou maior.")
            return
    except ValueError:
        print(f"Erro: O valor de 'p' ('{sys.argv[1]}') deve ser um número inteiro.")
        return

    arquivo_entrada = sys.argv[2]
    arquivo_saida = sys.argv[3]

    num_registros, runs_atuais = gerar_runs_iniciais(arquivo_entrada, p_ways)
    
    if not runs_atuais:
        print("Nenhuma sequência foi gerada. Encerrando.")
        return
        
    num_runs_iniciais = len(runs_atuais)
    num_passagens = 0
    
    run_counter = 0

    while len(runs_atuais) > 1:
        num_passagens += 1
        print(f"Iniciando Passagem de Intercalação #{num_passagens}...")
        runs_da_proxima_passagem = []
        
        for i in range(0, len(runs_atuais), p_ways):
            grupo_de_runs = runs_atuais[i : i + p_ways]
            
            run_counter += 1
            nome_run_saida = os.path.join(DIRETORIO_TEMP, f"merge_pass{num_passagens}_run{run_counter}.tmp")
            
            print(f"  Intercalando {len(grupo_de_runs)} runs para -> {nome_run_saida}")
            intercalar_runs(grupo_de_runs, nome_run_saida)
            runs_da_proxima_passagem.append(nome_run_saida)

        for run_antigo in runs_atuais:
            os.remove(run_antigo)
            
        runs_atuais = runs_da_proxima_passagem
    
    print("Intercalação finalizada.")

    if runs_atuais:
        arquivo_final_temporario = runs_atuais[0]
        os.rename(arquivo_final_temporario, arquivo_saida)
        print(f"Arquivo final '{arquivo_saida}' criado.")
    else:
        open(arquivo_saida, 'w').close()

    if os.path.exists(DIRETORIO_TEMP):
        for f in os.listdir(DIRETORIO_TEMP):
            os.remove(os.path.join(DIRETORIO_TEMP, f))
        os.rmdir(DIRETORIO_TEMP)
        
    print("Limpeza dos arquivos temporários concluída.")

    print("\n--- Estatísticas da Execução ---")
    print(f"#Regs: {num_registros}")
    print(f"#Ways: {p_ways}")
    print(f"#Runs: {num_runs_iniciais}")
    print(f"#Passes: {num_passagens}")


if __name__ == "__main__":
    main()