import { create } from 'zustand';

type FiltersState = {
  estado: string;
  distribuidora: string;
  segmento: string;
  busca: string;
  origem: string;
  setEstado: (uf: string) => void;
  setDistribuidora: (codigo: string) => void;
  setSegmento: (cnae: string) => void;
  setBusca: (texto: string) => void;
  setOrigem: (origem: string) => void;
  clearFilters: () => void;
};

export const useFilters = create<FiltersState>((set) => ({
  estado: '',
  distribuidora: '',
  segmento: '',
  busca: '',
  origem: '',
  setEstado: (uf: string) => set({ estado: uf }),
  setDistribuidora: (c: string) => set({ distribuidora: c }),
  setSegmento: (cnae: string) => set({ segmento: cnae }),
  setBusca: (texto: string) => set({ busca: texto }),
  setOrigem: (origem: string) => set({ origem }),
  clearFilters: (): void =>
    set({ estado: '', distribuidora: '', segmento: '', busca: '', origem: '' }),
}));
