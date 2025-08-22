# Sugestões de Melhorias — 22/08/2025 10:14

**Score geral:** 35/100

## 1. Avaliar se `ponto_notavel` deveria ter FKs
- Tipo: foreign key
- Motivo: Sem FKs — pode indicar acoplamento fraco ou falta de modelagem relacional

## 2. Avaliar se `modalidade_tarifaria` deveria ter FKs
- Tipo: foreign key
- Motivo: Sem FKs — pode indicar acoplamento fraco ou falta de modelagem relacional

## 3. Avaliar se `tipo_sistema` deveria ter FKs
- Tipo: foreign key
- Motivo: Sem FKs — pode indicar acoplamento fraco ou falta de modelagem relacional

## 4. Avaliar se `situacao_uc` deveria ter FKs
- Tipo: foreign key
- Motivo: Sem FKs — pode indicar acoplamento fraco ou falta de modelagem relacional

## 5. Avaliar se `classe_consumo` deveria ter FKs
- Tipo: foreign key
- Motivo: Sem FKs — pode indicar acoplamento fraco ou falta de modelagem relacional

## 6. Avaliar se `distribuidora` deveria ter FKs
- Tipo: foreign key
- Motivo: Sem FKs — pode indicar acoplamento fraco ou falta de modelagem relacional

## 7. Avaliar se `segmento_mercado` deveria ter FKs
- Tipo: foreign key
- Motivo: Sem FKs — pode indicar acoplamento fraco ou falta de modelagem relacional

## 8. Avaliar se `municipio` deveria ter FKs
- Tipo: foreign key
- Motivo: Sem FKs — pode indicar acoplamento fraco ou falta de modelagem relacional

## 9. Avaliar se `grupo_tensao` deveria ter FKs
- Tipo: foreign key
- Motivo: Sem FKs — pode indicar acoplamento fraco ou falta de modelagem relacional

## 10. Avaliar se `cnae` deveria ter FKs
- Tipo: foreign key
- Motivo: Sem FKs — pode indicar acoplamento fraco ou falta de modelagem relacional

## 11. Avaliar se `dataset_url_catalog` deveria ter FKs
- Tipo: foreign key
- Motivo: Sem FKs — pode indicar acoplamento fraco ou falta de modelagem relacional

## 12. Avaliar se `download_log` deveria ter FKs
- Tipo: foreign key
- Motivo: Sem FKs — pode indicar acoplamento fraco ou falta de modelagem relacional

## 13. Padronizar ou revisar coluna `ano`
- Tipo: coluna
- Motivo: Presente em 4 tabelas — possível desnormalização

## 14. Padronizar ou revisar coluna `lead_bruto_id`
- Tipo: coluna
- Motivo: Presente em 4 tabelas — possível desnormalização

## 15. Padronizar ou revisar coluna `origem`
- Tipo: coluna
- Motivo: Presente em 5 tabelas — possível desnormalização

## 16. Padronizar ou revisar coluna `import_id`
- Tipo: coluna
- Motivo: Presente em 5 tabelas — possível desnormalização

## 17. Padronizar ou revisar coluna `descricao`
- Tipo: coluna
- Motivo: Presente em 8 tabelas — possível desnormalização

## 18. Revisar granularidade de tipos em `modalidade_tarifaria`
- Tipo: coluna
- Motivo: Mais de 60% dos campos são genéricos (text/jsonb/varchar)

## 19. Revisar granularidade de tipos em `tipo_sistema`
- Tipo: coluna
- Motivo: Mais de 60% dos campos são genéricos (text/jsonb/varchar)

## 20. Revisar granularidade de tipos em `situacao_uc`
- Tipo: coluna
- Motivo: Mais de 60% dos campos são genéricos (text/jsonb/varchar)

## 21. Revisar granularidade de tipos em `classe_consumo`
- Tipo: coluna
- Motivo: Mais de 60% dos campos são genéricos (text/jsonb/varchar)

## 22. Revisar granularidade de tipos em `distribuidora`
- Tipo: coluna
- Motivo: Mais de 60% dos campos são genéricos (text/jsonb/varchar)

## 23. Revisar granularidade de tipos em `segmento_mercado`
- Tipo: coluna
- Motivo: Mais de 60% dos campos são genéricos (text/jsonb/varchar)

## 24. Revisar granularidade de tipos em `grupo_tensao`
- Tipo: coluna
- Motivo: Mais de 60% dos campos são genéricos (text/jsonb/varchar)

## 25. Revisar granularidade de tipos em `cnae`
- Tipo: coluna
- Motivo: Mais de 60% dos campos são genéricos (text/jsonb/varchar)

