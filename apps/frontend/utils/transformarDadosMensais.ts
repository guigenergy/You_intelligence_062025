import type { Lead } from '@/app/types/lead'

export function gerarDadosMensais(leads: Lead[]) {
  const energia = Array(12).fill(0);
  const demanda = Array(12).fill(0);
  const dic = Array(12).fill(0);
  const fic = Array(12).fill(0);

  const totalLeads = leads.length || 1;

  leads.forEach(lead => {
    for (let i = 1; i <= 12; i++) {
      energia[i-1] += Number((lead as any)[`ENE_${String(i).padStart(2, "0")}`]) || 0;
      demanda[i-1] += Number((lead as any)[`DEM_${String(i).padStart(2, "0")}`]) || 0;
      dic[i-1]     += Number((lead as any)[`DIC_${String(i).padStart(2, "0")}`]) || 0;
      fic[i-1]     += Number((lead as any)[`FIC_${String(i).padStart(2, "0")}`]) || 0;
    }
  });

  const energia_mensal = energia.map((valor, i) => ({
    mes: String(i + 1).padStart(2, "0"),
    energia_kwh: +(valor / totalLeads).toFixed(2)  
  }));

  const demanda_mensal = demanda.map((valor, i) => ({
    mes: String(i + 1).padStart(2, "0"),
    demanda_kw: +(valor / totalLeads).toFixed(2)
  }));

  const dic_fic_mensal = dic.map((valor, i) => ({
    mes: String(i + 1).padStart(2, "0"),
    dic: +(dic[i] / totalLeads).toFixed(2),
    fic: +(fic[i] / totalLeads).toFixed(2),
  }));

  return { energia_mensal, demanda_mensal, dic_fic_mensal };
}
