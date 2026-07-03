# VoX-Transcribe

VoX-Transcribe e uma ferramenta local/interna para transcricao de audios em lote,
com fila de processamento e suporte a agentes de execucao distribuidos.

O projeto nasceu de um script local baseado em Faster-Whisper e evolui para uma
arquitetura com API de borda, Core de orquestracao e Workers. A API Java atende
a interface e clientes externos; o Core Python gerencia tarefas, agentes e
execucao distribuida; os Workers executam o processamento pesado em Python.

## Requisitos

Requisitos de ambiente:

- Java: linguagem utilizada na API `vox-api`;
- Python: linguagem utilizada em `vox-core` e `vox-worker`;
- uv: gerenciador de dependências usado para os módulos em python;
- PostgreSQL: para persistir informações sobre as execuções; e
- ffmpeg: processamento de arquivos de aúdio do Faster-Whisper;
