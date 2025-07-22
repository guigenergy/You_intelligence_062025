export type Lead = {
  id: string
  dicMed: number
  ficMed: number
  cnae: string
  bairro: string
  cep: string
  estado: string
  distribuidora: string
  codigoDistribuidora: string 
  segmento: string
  descricao?: string
  potencia?: number; 
  longitude: number
  latitude: number
  tipo: string

  ENE_01?: number; ENE_02?: number; ENE_03?: number; ENE_04?: number; ENE_05?: number; ENE_06?: number;
  ENE_07?: number; ENE_08?: number; ENE_09?: number; ENE_10?: number; ENE_11?: number; ENE_12?: number;
  DEM_01?: number; DEM_02?: number; DEM_03?: number; DEM_04?: number; DEM_05?: number; DEM_06?: number;
  DEM_07?: number; DEM_08?: number; DEM_09?: number; DEM_10?: number; DEM_11?: number; DEM_12?: number;
  DIC_01?: number; DIC_02?: number; DIC_03?: number; DIC_04?: number; DIC_05?: number; DIC_06?: number;
  DIC_07?: number; DIC_08?: number; DIC_09?: number; DIC_10?: number; DIC_11?: number; DIC_12?: number;
  FIC_01?: number; FIC_02?: number; FIC_03?: number; FIC_04?: number; FIC_05?: number; FIC_06?: number;
  FIC_07?: number; FIC_08?: number; FIC_09?: number; FIC_10?: number; FIC_11?: number; FIC_12?: number;
}
