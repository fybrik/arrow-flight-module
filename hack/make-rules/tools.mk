INSTALL_TOOLS += $(TOOLBIN)/yq
$(TOOLBIN)/yq:
	cd $(TOOLS_DIR); ./install_yq.sh
	$(call post-install-check)

INSTALL_TOOLS += $(TOOLBIN)/helm
$(TOOLBIN)/helm:
	cd $(TOOLS_DIR); ./install_helm.sh
	$(call post-install-check)

INSTALL_TOOLS += $(TOOLBIN)/kind
$(TOOLBIN)/kind:
	GOBIN=$(ABSTOOLBIN) go install sigs.k8s.io/kind@v0.11.1
	$(call post-install-check)

INSTALL_TOOLS += $(TOOLBIN)/kubebuilder
$(TOOLBIN)/kubebuilder $(TOOLBIN)/etcd $(TOOLBIN)/kube-apiserver $(TOOLBIN)/kubectl:
	cd $(TOOLS_DIR); ./install_kubebuilder.sh
	$(call post-install-check)

.PHONY: install-tools
install-tools: $(INSTALL_TOOLS)

.PHONY: uninstall-tools
uninstall-tools:
	rm -rf $(INSTALL_TOOLS)



