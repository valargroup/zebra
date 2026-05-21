# Join the NU7 Testnet

Use this page on a fresh Ubuntu machine to download the NU7 join bundle and run
the join script in observer or mining mode.

The bundle URL defaults to `nu7-testnet/nu7-join-bundle.tar.gz` next to this
page. Add `?bundle_url=...` to the page URL to prefill a different bundle. If
the script and bundle are already on the machine, pass the local bundle path
instead.

Observer mode with local files:

```sh
bash join-nu7-testnet.sh --bundle-url ./nu7-join-bundle.tar.gz
```

Mining mode with local files:

```sh
bash join-nu7-testnet.sh --bundle-url ./nu7-join-bundle.tar.gz --mine
```

Mining mode with a supplied miner address:

```sh
bash join-nu7-testnet.sh --bundle-url ./nu7-join-bundle.tar.gz --mine --miner-address t...
```

<div class="nu7-join-page">
  <section class="nu7-join-controls" aria-label="Join command builder">
    <label for="bundle-url">Join bundle URL or local path</label>
    <input id="bundle-url" type="url" spellcheck="false" />
    <label for="miner-address">Miner address</label>
    <input id="miner-address" type="text" spellcheck="false" placeholder="Optional transparent testnet address" />
    <div class="nu7-mode-row" role="group" aria-label="Node mode">
      <label><input type="radio" name="join-mode" value="observer" checked /> Observer</label>
      <label><input type="radio" name="join-mode" value="mine" /> Mining</label>
    </div>
    <div class="nu7-action-row">
      <a id="download-bundle" class="nu7-button" href="nu7-testnet/nu7-join-bundle.tar.gz" download>Download bundle</a>
      <a id="download-script" class="nu7-button" href="nu7-testnet/join-nu7-testnet.sh" download>Download script</a>
      <button id="copy-command" type="button">Copy command</button>
      <button id="copy-script" type="button" disabled>Copy script</button>
    </div>
  </section>
  <h2>Run Command</h2>
  <pre><code id="join-command"></code></pre>
  <h2>Join Script</h2>
  <pre><code id="join-script"></code></pre>
</div>

<style>
.nu7-join-page {
  max-width: 960px;
}

.nu7-join-controls {
  display: grid;
  gap: 0.75rem;
  margin: 1rem 0 1.5rem;
}

.nu7-join-controls input[type="text"],
.nu7-join-controls input[type="url"] {
  box-sizing: border-box;
  width: 100%;
  padding: 0.5rem 0.625rem;
  border: 1px solid var(--table-border-color);
  border-radius: 4px;
  color: var(--fg);
  background: var(--bg);
  font: inherit;
}

.nu7-mode-row,
.nu7-action-row {
  display: flex;
  flex-wrap: wrap;
  gap: 0.625rem;
  align-items: center;
}

.nu7-mode-row label {
  display: inline-flex;
  gap: 0.35rem;
  align-items: center;
}

.nu7-action-row button,
.nu7-button {
  display: inline-flex;
  align-items: center;
  min-height: 2.25rem;
  padding: 0.375rem 0.75rem;
  border: 1px solid var(--table-border-color);
  border-radius: 4px;
  color: var(--fg);
  background: var(--bg);
  font: inherit;
  text-decoration: none;
  cursor: pointer;
}

.nu7-action-row button:hover,
.nu7-button:hover {
  border-color: var(--links);
  color: var(--links);
}

.nu7-action-row button:disabled {
  cursor: not-allowed;
  opacity: 0.55;
}
</style>

<script>
(function () {
  const defaultBundleUrl = new URL("nu7-testnet/nu7-join-bundle.tar.gz", window.location.href).href;
  const scriptUrl = new URL("nu7-testnet/join-nu7-testnet.sh", window.location.href).href;
  const params = new URLSearchParams(window.location.search);

  const bundleInput = document.getElementById("bundle-url");
  const minerAddressInput = document.getElementById("miner-address");
  const commandOutput = document.getElementById("join-command");
  const scriptOutput = document.getElementById("join-script");
  const downloadBundle = document.getElementById("download-bundle");
  const copyCommand = document.getElementById("copy-command");
  const copyScript = document.getElementById("copy-script");

  bundleInput.value = params.get("bundle_url") || params.get("bundle") || defaultBundleUrl;
  minerAddressInput.value = params.get("miner_address") || "";

  if (params.get("mode") === "mine") {
    document.querySelector("input[name='join-mode'][value='mine']").checked = true;
  }

  function shellQuote(value) {
    return "'" + value.replace(/'/g, "'\"'\"'") + "'";
  }

  function modeArgs() {
    const mode = document.querySelector("input[name='join-mode']:checked").value;
    const minerAddress = minerAddressInput.value.trim();

    if (mode !== "mine") {
      return "";
    }

    return minerAddress ? " --mine --miner-address " + shellQuote(minerAddress) : " --mine";
  }

  function updateCommand() {
    const bundleUrl = bundleInput.value.trim() || defaultBundleUrl;
    downloadBundle.href = bundleUrl;

    commandOutput.textContent = [
      "curl -fsSLO " + shellQuote(scriptUrl),
      "bash join-nu7-testnet.sh --bundle-url " + shellQuote(bundleUrl) + modeArgs(),
    ].join("\n");
  }

  function setCopied(button) {
    const originalText = button.textContent;
    button.textContent = "Copied";
    window.setTimeout(function () {
      button.textContent = originalText;
    }, 1400);
  }

  async function copyText(button, text) {
    if (navigator.clipboard) {
      await navigator.clipboard.writeText(text);
    } else {
      const textarea = document.createElement("textarea");
      textarea.value = text;
      textarea.setAttribute("readonly", "");
      textarea.style.position = "fixed";
      textarea.style.opacity = "0";
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand("copy");
      textarea.remove();
    }

    setCopied(button);
  }

  fetch(scriptUrl)
    .then(function (response) {
      if (!response.ok) {
        throw new Error("script fetch failed");
      }

      return response.text();
    })
    .then(function (script) {
      scriptOutput.textContent = script;
      copyScript.disabled = false;
    })
    .catch(function () {
      scriptOutput.textContent = "Unable to load " + scriptUrl;
      copyScript.disabled = true;
    });

  bundleInput.addEventListener("input", updateCommand);
  minerAddressInput.addEventListener("input", updateCommand);
  document.querySelectorAll("input[name='join-mode']").forEach(function (input) {
    input.addEventListener("change", updateCommand);
  });

  copyCommand.addEventListener("click", function () {
    copyText(copyCommand, commandOutput.textContent);
  });

  copyScript.addEventListener("click", function () {
    copyText(copyScript, scriptOutput.textContent);
  });

  updateCommand();
})();
</script>
