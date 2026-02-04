import { AwsClient } from "aws4fetch";

const HOMEPAGE = "https://github.com/milkey-mouse/git-lfs-s3-proxy";
const EXPIRY = 3600;

const MIME = "application/vnd.git-lfs+json";

const METHOD_FOR = {
  upload: "PUT",
  download: "GET",
};

async function sign(s3, bucket, path, method) {
  const info = { method };
  const signed = await s3.sign(
    new Request(`https://${bucket}/${path}?X-Amz-Expires=${EXPIRY}`, info),
    { aws: { signQuery: true } },
  );
  return signed.url;
}

function parseAuthorization(req) {
  const auth = req.headers.get("Authorization");
  if (!auth) {
    throw new Response(null, { status: 401 });
  }

  const [scheme, encoded] = auth.split(" ");
  if (scheme !== "Basic" || !encoded) {
    throw new Response(null, { status: 400 });
  }

  const buffer = Uint8Array.from(atob(encoded), (c) => c.charCodeAt(0));
  const decoded = new TextDecoder().decode(buffer);
  const index = decoded.indexOf(":");
  if (index === -1) {
    throw new Response(null, { status: 400 });
  }

  return { user: decoded.slice(0, index), pass: decoded.slice(index + 1) };
}

function getAwsCredentials(req, env) {
  if (env.USE_CLOUDFLARE_CREDS === "true") {
    if (!env.AWS_ACCESS_KEY_ID || !env.AWS_SECRET_ACCESS_KEY) {
      throw new Response("Missing Cloudflare AWS credentials", { status: 500 });
    }

    return {
      accessKeyId: env.AWS_ACCESS_KEY_ID,
      secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
      sessionToken: env.AWS_SESSION_TOKEN,
    };
  }

  const { user, pass } = parseAuthorization(req);
  return { accessKeyId: user, secretAccessKey: pass };
}

async function fetch(req, env) {
  const url = new URL(req.url);

  if (url.pathname == "/") {
    if (req.method === "GET") {
      return Response.redirect(HOMEPAGE, 302);
    } else {
      return new Response(null, { status: 405, headers: { Allow: "GET" } });
    }
  }

  if (!url.pathname.endsWith("/objects/batch")) {
    return new Response(null, { status: 404 });
  }

  if (req.method !== "POST") {
    return new Response(null, { status: 405, headers: { Allow: "POST" } });
  }


  let s3Options = getAwsCredentials(req, env);

  const segments = url.pathname.split("/").slice(1, -2);
  let params = {};
  let bucketIdx = 0;
  for (const segment of segments) {
    const sliceIdx = segment.indexOf("=");
    if (sliceIdx === -1) {
      break;
    } else {
      const key = decodeURIComponent(segment.slice(0, sliceIdx));
      const val = decodeURIComponent(segment.slice(sliceIdx + 1));
      s3Options[key] = val;

      bucketIdx++;
    }
  }

  const s3 = new AwsClient(s3Options);
  const bucket = segments.slice(bucketIdx).join("/");
  const expires_in = params.expiry || env.EXPIRY || EXPIRY;

  const { objects, operation, hash_algo = "sha256" } = await req.json();

  if (hash_algo !== "sha256") {
    return new Response(
      JSON.stringify({
        message: `Hash algorithm '${hash_algo}' is not supported. Only 'sha256' is currently supported.`,
      }),
      {
        status: 409,
        headers: { "Content-Type": "application/vnd.git-lfs+json" },
      },
    );
  }

  const method = METHOD_FOR[operation];
  const response = JSON.stringify({
    transfer: "basic",
    hash_algo: "sha256",
    objects: await Promise.all(
      objects.map(async ({ oid, size }) => ({
        oid,
        size,
        authenticated: true,
        actions: {
          [operation]: {
            href: await sign(s3, bucket, oid, method),
            expires_in,
          },
        },
      })),
    ),
  });

  return new Response(response, {
    status: 200,
    headers: {
      "Cache-Control": "no-store",
      "Content-Type": "application/vnd.git-lfs+json",
    },
  });
}

export default { fetch };
