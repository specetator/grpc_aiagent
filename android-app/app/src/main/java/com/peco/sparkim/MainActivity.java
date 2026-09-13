package com.peco.sparkim;

import android.app.Activity;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.webkit.*;
import android.view.KeyEvent;
import android.view.ViewGroup;

/** Thin Android channel shell. The existing WebDemo remains the shared UI/channel implementation. */
public final class MainActivity extends Activity {
    private WebView webView;
    private ValueCallback<Uri[]> uploadCallback;
    private static final int FILE_CHOOSER = 42;
    @Override public void onCreate(Bundle state) { super.onCreate(state);
        webView = new WebView(this); webView.setLayoutParams(new ViewGroup.LayoutParams(-1,-1));
        WebSettings s=webView.getSettings(); s.setJavaScriptEnabled(true); s.setDomStorageEnabled(true);
        s.setAllowFileAccess(false); s.setAllowContentAccess(true); s.setMediaPlaybackRequiresUserGesture(false);
        webView.setWebChromeClient(new WebChromeClient() { @Override public boolean onShowFileChooser(WebView v, ValueCallback<Uri[]> cb, FileChooserParams p) { if(uploadCallback!=null) uploadCallback.onReceiveValue(null); uploadCallback=cb; startActivityForResult(p.createIntent(), FILE_CHOOSER); return true; } });
        webView.setWebViewClient(new WebViewClient()); setContentView(webView);
        String url=getIntent().getStringExtra("url"); if(url==null) url=getString(com.peco.sparkim.R.string.server_url); webView.loadUrl(url);
    }
    @Override protected void onActivityResult(int r,int c,Intent d) { super.onActivityResult(r,c,d); if(r==FILE_CHOOSER && uploadCallback!=null){ uploadCallback.onReceiveValue(WebChromeClient.FileChooserParams.parseResult(c,d)); uploadCallback=null; } }
    @Override public boolean onKeyDown(int key,KeyEvent e){ if(key==KeyEvent.KEYCODE_BACK && webView.canGoBack()){webView.goBack();return true;} return super.onKeyDown(key,e); }
    @Override protected void onDestroy(){ if(webView!=null) webView.destroy(); super.onDestroy(); }
}
