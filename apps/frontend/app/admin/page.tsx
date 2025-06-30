'use client';

import { useState } from 'react';
import SelectDistribuidoras from './SelectDistribuidoras';
import SelectAnos from './SelectAnos';
import ButtonImportar from './ButtonImportar';
import TabelaStatusImportacoes from './TabelaStatusImportacoes';
import PainelEnriquecimento from './PainelEnriquecimento';

export default function AdminPage() {
  const [distribuidorasSelecionadas, setDistribuidorasSelecionadas] = useState<string[]>([]);
  const [anosSelecionados, setAnosSelecionados] = useState<number[]>([]);

  return (
    <div className="bg-zinc-900 border border-zinc-700 text-white rounded-xl shadow p-6">
      <h1 className="text-4xl font-bold text-center text-white mb-10">

        📊 Painel de Administração de Dados
      </h1>



      {/* Importação */}
      <section className="mb-10">
        <div className="flex flex-wrap gap-6 items-end bg-zinc-900 border border-zinc-700 text-white p-6 rounded-xl shadow w-fit mb-8">
        <SelectDistribuidoras onChange={setDistribuidorasSelecionadas} />
        <SelectAnos onChange={setAnosSelecionados} />
        <ButtonImportar distribuidoras={distribuidorasSelecionadas} anos={anosSelecionados} />
        </div>

      </section>

      {/* Status de importações */}
      <section className="mb-10">
        <TabelaStatusImportacoes />
      </section>

      {/* Enriquecimento */}
      <section className="mb-10">
        <PainelEnriquecimento />
      </section>
    </div>
  );
}
