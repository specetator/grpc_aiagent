package com.peco.sparkim;

import android.app.Activity;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.webkit.*;
import android.view.KeyEvent;
import android.view.ViewGroup;
import android.view.Gravity;
import android.widget.*;
import android.content.SharedPreferences;

/** Thin Android channel shell. The existing WebDemo remains the shared UI/channel implementation. */
public final class MainActivity extends Activity {
    private WebView webView;
    private ValueCallback<Uri[]> uploadCallback;
    private static final int FILE_CHOOSER = 42;
    @Override public void onCreate(Bundle state) { super.onCreate(state);
        String requested=getIntent().getStringExtra("url");
        String saved=getSharedPreferences("spark",MODE_PRIVATE).getString("url", "");
        String url=requested!=null ? requested : saved;
        if (url==null || url.trim().isEmpty()) { showServerSetup(); return; }
        openWeb(url.trim());
    }
    private void showServerSetup() {
        LinearLayout box=new LinearLayout(this); box.setOrientation(LinearLayout.VERTICAL); box.setPadding(40,80,40,40); box.setGravity(Gravity.CENTER_VERTICAL);
        TextView title=new TextView(this); title.setText("Spark IM"); title.setTextSize(28); box.addView(title);
        TextView hint=new TextView(this); hint.setText("输入 PC 的 Tailscale 地址，例如 https://pc-name.your-tailnet.ts.net:9010/index.html"); hint.setPadding(0,20,0,12); box.addView(hint);
        EditText input=new EditText(this); input.setSingleLine(true); input.setHint("https://..."); box.addView(input,new LinearLayout.LayoutParams(-1,-2));
        Button connect=new Button(this); connect.setText("连接"); box.addView(connect);
        connect.setOnClickListener(v->{String value=input.getText().toString().trim(); if(!value.startsWith("http://")&&!value.startsWith("https://")){input.setError("请输入 http:// 或 https:// 地址");return;} getSharedPreferences("spark",MODE_PRIVATE).edit().putString("url",value).apply(); openWeb(value);});
        setContentView(box);
    }
    private void openWeb(String url) {
        webView = new WebView(this); webView.setLayoutParams(new ViewGroup.LayoutParams(-1,-1));
        WebSettings s=webView.getSettings(); s.setJavaScriptEnabled(true); s.setDomStorageEnabled(true);
        s.setAllowFileAccess(false); s.setAllowContentAccess(true); s.setMediaPlaybackRequiresUserGesture(false);
        webView.setWebChromeClient(new WebChromeClient() { @Override public boolean onShowFileChooser(WebView v, ValueCallback<Uri[]> cb, FileChooserParams p) { if(uploadCallback!=null) uploadCallback.onReceiveValue(null); uploadCallback=cb; startActivityForResult(p.createIntent(), FILE_CHOOSER); return true; } });
        webView.setWebViewClient(new WebViewClient()); setContentView(webView);
        webView.loadUrl(url);
    }
    @Override protected void onActivityResult(int r,int c,Intent d) { super.onActivityResult(r,c,d); if(r==FILE_CHOOSER && uploadCallback!=null){ uploadCallback.onReceiveValue(WebChromeClient.FileChooserParams.parseResult(c,d)); uploadCallback=null; } }
    @Override public boolean onKeyDown(int key,KeyEvent e){ if(key==KeyEvent.KEYCODE_BACK && webView.canGoBack()){webView.goBack();return true;} return super.onKeyDown(key,e); }
    @Override protected void onDestroy(){ if(webView!=null) webView.destroy(); super.onDestroy(); }
}
