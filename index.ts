import { Client } from "@botpress/client";
import { Zai } from "@botpress/zai";
import { z } from "@bpinternal/zui";

const BOT_ID = process.env.BOT_ID;
const TOKEN = process.env.TOKEN;
const KB_ID = process.env.KB_ID;

const MAX_CATEGORIES = 250;
const MAX_FILES_TO_PROCESS = 100; // Make this 999999 to process all files

const client = new Client({
  botId: BOT_ID,
  token: TOKEN,
});

const zai = new Zai({ client });

const cacheKey = `kb-analysis-cache/${KB_ID}.jsonl`;
const fileContentCache = new Map<string, string>();

/** Saves the progress as JSONL in the file API, so we can resume where we left off if needed */
async function saveCache() {
  const fileContent = Array.from(fileContentCache.entries())
    .map(([key, content]) => JSON.stringify({ key, content }))
    .join("\n");

  await client.uploadFile({
    key: cacheKey,
    content: fileContent,
    tags: {},
  });

  console.log(`Cache saved to ${cacheKey} (${fileContentCache.size} items)`);
}

/** Loads the progress as JSONL in the file API, so we can resume where we left off if needed */
async function restoreCache() {
  try {
    const { file } = await client.getFile({ id: cacheKey });
    await fetch(file.url)
      .then((x) => x.text())
      .then((x) => {
        const lines = x.split("\n");
        for (const line of lines) {
          if (line) {
            const { key, content } = JSON.parse(line);
            fileContentCache.set(key, content);
          }
        }
      });
    console.log(
      `Cache restored from ${cacheKey} (${fileContentCache.size} items)`
    );
  } catch {}
}

async function fetchAllContent() {
  const files = await client.list
    .files({
      sortDirection: "desc",
      sortField: "updatedAt",
      tags: {
        source: "knowledge-base",
        kbId: KB_ID,
      },
    })
    .collect();

  console.log("Total files: ", files.length);

  let fileCount = 0;
  let newProcessedFiles = 0;

  for (const file of files) {
    fileCount++;

    if (fileCount > MAX_FILES_TO_PROCESS) {
      console.log(
        `Reached max files to process (${MAX_FILES_TO_PROCESS}), stopping...`
      );
      break;
    }

    console.log(`Processing file ${fileCount} of ${files.length}: ${file.key}`);

    if (fileContentCache.has(file.key)) {
      console.log(`File ${file.key} already processed, skipping...`);
      continue;
    }

    const content = await client.list
      .filePassages({ id: file.id })
      .collect()
      .then((x) =>
        // Put the passages back in the right order
        x.sort((a, b) => (a.meta?.position ?? 0) - (b.meta?.position ?? 0))
      )
      .then((x) => x.map((p) => p.content).join("\n"));

    fileContentCache.set(file.key, content);
    if (newProcessedFiles++ % 10 === 0) {
      await saveCache();
    }
  }

  if (newProcessedFiles > 0) {
    console.log(`Processed ${newProcessedFiles} new files`);
    await saveCache();
  }
}

async function generateListOfCategories() {
  const allContent = fileContentCache.values().toArray().join("\n");
  const categoriesAsText = await zai.summarize(allContent, {
    format: `List of categories/labels like: - category1\n- category2\n- category3 (up to ${MAX_CATEGORIES})`,
    prompt: `You are a generator of categories. Your task is to analyze the content and generate a list of categories / labels to tag the content with. You need to generate a maximum of ${MAX_CATEGORIES} categories. The categories should be relevant to the content and should be in the form of a list.`,
  });

  const categoriesAsArray = await zai.extract(
    categoriesAsText,
    z.array(z.object({ name: z.string() })),
    {
      instructions: "Extract the categories from the text",
    }
  );

  const categories = categoriesAsArray
    .map((x) => x.name.trim())
    .filter(Boolean);

  console.log(
    `Found ${categories.length} categories:`,
    JSON.stringify(categories, null, 2)
  );

  return categories;
}

async function labelFilesFromCategories(categories: string[]) {
  for (const [key, content] of fileContentCache.entries()) {
    const labels = categories.map((x) => [
      x.replace(/[^A-Za-z0-9]/g, "_").replace(/_+/g, "_"),
      x,
    ]);

    const labelsToApply = await zai.label(content, Object.fromEntries(labels), {
      instructions:
        "Label the content with the categories. Only apply the most relevant labels. Do not apply labels that are not relevant.",
    });

    console.log(`File ${key} labels:`, labelsToApply);
    if (labels.length > 0) {
      await client.updateFileMetadata({
        id: key,
        metadata: {
          categories: labels,
        },
      });
    }
  }
}

async function main() {
  await restoreCache();
  await fetchAllContent();
  const categories = await generateListOfCategories();
  await labelFilesFromCategories(categories);
}

void main();
