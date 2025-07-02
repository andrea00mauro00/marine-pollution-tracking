from pyflink.datastream import StreamExecutionEnvironment

def main():
    # Crea un ambiente di esecuzione
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Crea un semplice flusso di dati da una collezione
    ds = env.from_collection(
        collection=[(1, 'Hello'), (2, 'World')],
        type_info=None
    )
    
    # Stampa i risultati
    ds.print()
    
    # Esegui il job
    env.execute("Simple Test Job")

if __name__ == "__main__":
    main()
