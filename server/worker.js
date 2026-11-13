    const reqUrl = new URL(request.url);
    if (reqUrl.pathname === "/recomp.wasm") {
      const wasmRes = await fetch("https://github.com");
      return new Response(wasmRes.body, {
        headers: {
          "Content-Type": "application/wasm",
          "Cross-Origin-Resource-Policy": "cross-origin",
          "Access-Control-Allow-Origin": "*"
        }
      });
    }
