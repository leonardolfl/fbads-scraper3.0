// update-eagle-json.mjs
import fs from "fs";
import { supabase } from "./supabase.js";

const fetchFn = globalThis.fetch || (await import("node-fetch")).default;

async function main() {
  console.log("🦅 Iniciando atualização do eagle_offers_data.json...");

  const { data: offers, error } = await supabase
    .from("swipe_file_offers")
    .select("*")
    .order("updated_at", { ascending: false });

  if (error) {
    console.error("❌ Erro ao buscar ofertas do Supabase:", error.message);
    process.exit(1);
  }

  if (!offers || offers.length === 0) {
    console.log("⚠️ Nenhuma oferta encontrada no banco. Abortando atualização.");
    process.exit(0);
  }

  // Mantém null literal (não string)
  const formatted = {
    version: new Date().toISOString().slice(0, 10),
    offers: offers.map((o) => ({
      id: o.id ?? null,
      offerName: o.offerName ?? null,
      niche: o.niche ?? null,
      activeAds: typeof o.activeAds === "number" ? o.activeAds : null,
      location: o.location ?? null,
      funnel: o.funnel ?? null,
      deliverable: o.deliverable ?? null,
      ticket: o.ticket ?? null,
      dateAdded: o.dateAdded ?? o.created_at ?? null,
      adLibraryUrl: o.adLibraryUrl ?? null,
      pageUrl: o.pageUrl ?? null,
      checkoutUrl: o.checkoutUrl ?? null,
    })),
  };

  const json = JSON.stringify(formatted, null, 2);
  fs.writeFileSync("eagle_offers_data.json", json);
  console.log(`✅ Gerado arquivo local com ${offers.length} ofertas.`);

  const uploadUrl = process.env.EAGLE_UPDATE_URL;
  if (!uploadUrl) {
    console.error("❌ Variável EAGLE_UPDATE_URL não definida nos secrets.");
    process.exit(1);
  }

  console.log(`🌐 Enviando JSON atualizado para ${uploadUrl} ...`);

  try {
    const res = await fetchFn(uploadUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: json,
    });

    const text = await res.text();
    if (!res.ok) {
      console.error(`❌ Falha no upload: ${res.status} ${res.statusText}`);
      console.error("Resposta do servidor:", text);
      process.exit(1);
    }

    console.log("✅ Upload concluído com sucesso!");
    console.log("🔍 Resposta do servidor:", text);
  } catch (err) {
    console.error("❌ Erro ao enviar arquivo:", err.message);
    process.exit(1);
  }
}

main();
