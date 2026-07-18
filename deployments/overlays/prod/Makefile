# Deploy the prod overlay.
#
# The distill-prompt ConfigMap references prompts/distillation_api.md (the single
# repo copy, which lives above deployments/base), so kustomize's default load
# restriction blocks the build. These targets relax it so nobody has to remember
# the flag. Note: `kubectl apply -k` does NOT accept --load-restrictor, so we
# render with `kubectl kustomize` (which does) and pipe the result into apply.

KUBECTL ?= kubectl
DIR := .
RESTRICTOR := --load-restrictor LoadRestrictionsNone

.PHONY: render apply diff delete

# render: print the rendered manifests (sanity-check before applying).
render:
	$(KUBECTL) kustomize $(RESTRICTOR) $(DIR)

# apply: deploy to the current kube-context.
apply:
	$(KUBECTL) kustomize $(RESTRICTOR) $(DIR) | $(KUBECTL) apply -f -

# diff: show what apply would change against the live cluster.
diff:
	$(KUBECTL) kustomize $(RESTRICTOR) $(DIR) | $(KUBECTL) diff -f -

# delete: remove this overlay's resources.
delete:
	$(KUBECTL) kustomize $(RESTRICTOR) $(DIR) | $(KUBECTL) delete -f -
