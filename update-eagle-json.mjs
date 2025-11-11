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

  const formatted = {
    version: new Date().toISOString().slice(0, 10),
    offers: offers.map((o) => ({
      id: o.id,
      offerName: o.offerName,
      niche: o.niche || "",
      activeAds: o.activeAds ?? 0,
      location: o.location || "",
      funnel: o.funnel || "",
      deliverable: o.deliverable || "",
      ticket: o.ticket || "",
      dateAdded: o.dateAdded || o.created_at || null,
      adLibraryUrl: o.adLibraryUrl || "",
      pageUrl: o.pageUrl || "",
      checkoutUrl: o.checkoutUrl || "",
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
